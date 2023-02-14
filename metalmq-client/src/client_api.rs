use crate::{
    channel_api::{Channel, ChannelState},
    consumer::{ConsumerSignal, GetSignal},
    message::ReturnedMessage,
    model::ChannelNumber,
    processor::{self, ClientRequest, ClientRequestSink, Param},
};
use anyhow::{anyhow, Result};
use metalmq_codec::frame;
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

pub(crate) type ConsumerSink = mpsc::UnboundedSender<ConsumerSignal>;
pub(crate) type GetSink = mpsc::UnboundedSender<GetSignal>;
pub(crate) type ConnectionSink = mpsc::UnboundedSender<EventSignal>;

/// Listening notifications sent by the server like channel closed or message publish acknowledged
/// in confirm mode.
#[derive(Debug)]
pub enum EventSignal {
    /// Message publish acknowledged.
    BasicAck {
        channel: ChannelNumber,
        delivery_tag: u64,
        multiple: bool,
    },
    /// Message cannot be published (no route or no consumer).
    BasicReturn {
        channel: ChannelNumber,
        message: Box<ReturnedMessage>,
    },
    /// Channel closed
    ChannelClose,
    /// Connection closed
    ConnectionClose,
}

/// The AMQP client instance which reprensents an open connection.
///
/// `Client` can be created with [`Client::connect`].
///
/// # Usage
///
/// ```no_run
/// use metalmq_client::*;
///
/// async fn connect() {
///     let mut client = Client::connect("localhost:5672", "guest", "guest").await.unwrap();
///     let mut channel = client.channel_open(1u16).await.unwrap();
///
///     // Start a new thread to handle events
///     //tokio::spawn(async move {
///     //    use EventSignal::*;
///
///     //    while let Some(evt) = client.event_stream.recv().await {
///     //        match evt {
///     //            ChannelClose => panic!("Handle it better"),
///     //            _ => (),
///     //        }
///     //    }
///     //});
///     // ...
///     channel.close().await.unwrap();
///     client.close().await.unwrap();
/// }
/// ```
pub struct Client {
    /// The id of the connection.
    pub connection_id: String,
    /// The stream of connection events which can happen any time like cancellation of a consumer
    /// or closing of a connection.
    pub event_stream: mpsc::UnboundedReceiver<EventSignal>,
    request_sink: mpsc::Sender<ClientRequest>,
    /// Sync calls register here per channel (0 for connection related frames). Once response frame
    /// arrives, the oneshot channel is notified. Channel close and connection close events should
    /// unblock the waiting tasks here by notifying them with an error or a unit reply.
    channels: HashMap<u16, Channel>,
    next_channel: u16,
}

/// Create a connection to an AMQP server and returns a sink to send the requests.
async fn create_connection(url: &str, conn_sink: ConnectionSink) -> Result<ClientRequestSink> {
    use tokio::net::TcpStream;

    match TcpStream::connect(url).await {
        Ok(socket) => {
            let (sender, receiver) = mpsc::channel(1);

            tokio::spawn(async move {
                if let Err(e) = processor::start_loop_and_output(socket, receiver, conn_sink).await {
                    eprintln!("error: {:?}", e);
                }
            });

            Ok(sender)
        }
        Err(e) => Err(anyhow!("Connection error {:?}", e)),
    }
}

impl Client {
    // TODO expect only one parameter and parse the url
    /// The main entry point of the API, it connects to an AMQP compatible server.
    pub async fn connect(url: &str, username: &str, password: &str) -> Result<Client> {
        let (conn_evt_tx, conn_evt_rx) = mpsc::unbounded_channel();
        let (connected_tx, connected_rx) = oneshot::channel();
        let client_sink = create_connection(url, conn_evt_tx).await?;

        client_sink
            .send(ClientRequest {
                param: Param::Connect {
                    username: username.to_owned(),
                    password: password.to_owned(),
                    virtual_host: "/".to_owned(),
                    connected: connected_tx,
                },
                response: None,
            })
            .await?;

        connected_rx.await?;

        Ok(Client {
            connection_id: "01234".to_owned(),
            request_sink: client_sink,
            event_stream: conn_evt_rx,
            channels: HashMap::new(),
            next_channel: 1u16,
        })
    }

    /// Open a channel in the current connection.
    pub async fn channel_open(&mut self, channel: ChannelNumber) -> Result<Channel> {
        processor::call(&self.request_sink, frame::channel_open(channel)).await?;

        Ok(Channel::new(channel, self.request_sink.clone()))
    }

    /// Open a channel with the next available channel number.
    pub async fn channel_open_next(&mut self) -> Result<Channel> {
        let channel_number = self.next_channel;

        self.next_channel += 1;

        self.channel_open(channel_number).await
    }

    /// Closes the channel normally.
    pub async fn close(&mut self) -> Result<()> {
        for (_, mut channel) in self.channels.drain() {
            if let ChannelState::Open = channel.state {
                channel.close().await?;
            }
        }

        let fr = frame::connection_close(200, "Normal close", 0);
        processor::call(&self.request_sink, fr).await?;

        Ok(())
    }

    /// Convenient function for listening `EventSignal` with timeout.
    pub async fn receive_event(&mut self, timeout: std::time::Duration) -> Option<EventSignal> {
        let sleep = tokio::time::sleep(timeout);
        tokio::pin!(sleep);

        tokio::select! {
            signal = self.event_stream.recv() => {
                signal
            }
            _ = &mut sleep => {
                None
            }
        }
    }
}
