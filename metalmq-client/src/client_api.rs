use crate::{
    channel_api::Channel,
    consumer::ConsumerSignal,
    message::ReturnedMessage,
    model::ChannelNumber,
    processor::{self, ClientRequest, ClientRequestSink, Param, WaitFor},
};
use anyhow::{anyhow, Result};
use log::error;
use metalmq_codec::frame;
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

pub(crate) type ConsumerSink = mpsc::UnboundedSender<ConsumerSignal>;
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
        message: ReturnedMessage,
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
/// use metalmq_client::Client;
///
/// async fn connect() {
///     let mut client = Client::connect("localhost:5672", "guest", "guest").await.unwrap();
///     let mut channel = client.channel_open(1u16).await.unwrap();
///     // ...
///     channel.close().await.unwrap();
///     client.close().await.unwrap();
/// }
/// ```
pub struct Client {
    pub connection_id: String,
    pub event_stream: mpsc::UnboundedReceiver<EventSignal>,
    request_sink: mpsc::Sender<ClientRequest>,
    /// Sync calls register here per channel (0 for connection related frames). Once response frame
    /// arrives, the oneshot channel is notified. Channel close and connection close events should
    /// unblock the waiting tasks here by notifying them with an error or a unit reply.
    sync_calls: HashMap<u16, oneshot::Sender<Result<()>>>,
    channels: HashMap<u16, Channel>,
}

/// Create a connection to an AMQP server and returns a sink to send the requests.
async fn create_connection(url: &str, conn_sink: ConnectionSink) -> Result<ClientRequestSink> {
    use tokio::net::TcpStream;

    match TcpStream::connect(url).await {
        Ok(socket) => {
            let (sender, receiver) = mpsc::channel(1);

            tokio::spawn(async move {
                if let Err(e) = processor::start_loop_and_output(socket, receiver, conn_sink).await {
                    error!("error: {:?}", e);
                }
            });

            Ok(sender)
        }
        Err(e) => Err(anyhow!("Connection error {:?}", e)),
    }
}

impl Client {
    // TODO expect only one parameter and parse the url
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
                response: WaitFor::Nothing,
            })
            .await?;

        connected_rx.await?;

        Ok(Client {
            connection_id: "01234".to_owned(),
            request_sink: client_sink,
            event_stream: conn_evt_rx,
            sync_calls: HashMap::new(),
            channels: HashMap::new(),
        })
    }

    // TODO open a channel with the next channel number
    /// Open a channel in the current connection.
    pub async fn channel_open(&mut self, channel: ChannelNumber) -> Result<Channel> {
        processor::call(&self.request_sink, frame::channel_open(channel)).await?;

        Ok(Channel::new(channel, self.request_sink.clone()))
    }

    /// Closes the channel normally.
    pub async fn close(&mut self) -> Result<()> {
        let fr = frame::connection_close(200, "Normal close", 0);

        processor::call(&self.request_sink, fr).await?;

        Ok(())
    }

    /// Convenient function for listening `EventSignal` with timeout.
    pub async fn receive_event(&mut self, timeout: std::time::Duration) -> Option<EventSignal> {
        let sleep = tokio::time::sleep(tokio::time::Duration::from(timeout));
        tokio::pin!(sleep);

        tokio::select! {
            signal = self.event_stream.recv() => {
                signal
            }
            _ = &mut sleep => {
                return None;
            }
        }
    }
}
