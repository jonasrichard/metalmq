use crate::channel_api::{Channel, DeliveredContent};
use crate::consumer::ConsumerSignal;
use crate::model::ChannelNumber;
use crate::processor;
use anyhow::{anyhow, Result};
//use rand::prelude::*;
use log::error;
use metalmq_codec::frame;
use std::collections::HashMap;
use std::fmt;
use tokio::sync::{mpsc, oneshot};

pub(crate) type ClientRequestSink = mpsc::Sender<ClientRequest>;
pub(crate) type ConsumerSink = mpsc::UnboundedSender<ConsumerSignal>;

// TODO onBasicReturn callback
// TODO onChannelClose callback
// TODO onConnectionClose callback
//   where we should execute them? On a separate tokio thread?

/// Represent a client request. It can be sending a frame, consume a queue or publish data.
pub(crate) enum Param {
    Connect {
        username: String,
        password: String,
        virtual_host: String,
        connected: oneshot::Sender<()>,
    },
    Frame(frame::AMQPFrame),
    Consume(frame::AMQPFrame, ConsumerSink),
    Publish(frame::AMQPFrame, Vec<u8>),
}

pub(crate) type FrameResponse = oneshot::Sender<Result<()>>;

pub(crate) enum WaitFor {
    Nothing,
    SentOut(oneshot::Sender<Result<()>>),
    FrameResponse(FrameResponse),
}

/// Represents a client request, typically send a frame and wait for the answer of the server.
pub(crate) struct ClientRequest {
    pub(crate) param: Param,
    pub(crate) response: WaitFor,
}

impl fmt::Debug for ClientRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.param {
            Param::Connect {
                username, virtual_host, ..
            } => write!(f, "Request{{Connect={:?},{:?}}}", username, virtual_host),
            Param::Frame(frame) => write!(f, "Request{{Frame={:?}}}", frame),
            Param::Consume(frame, _) => write!(f, "Request{{Consume={:?}}}", frame),
            Param::Publish(frame, _) => write!(f, "Request{{Publish={:?}}}", frame),
        }
    }
}

/// The AMQP client instance which reprensents an open connection.
///
/// `Client` can be created with [`Client::connect`].
pub struct Client {
    pub connection_id: String,
    request_sink: mpsc::Sender<ClientRequest>,
    /// Sync calls register here per channel (0 for connection related frames). Once response frame
    /// arrives, the oneshot channel is notified. Channel close and connection close events should
    /// unblock the waiting tasks here by notifying them with an error or a unit reply.
    sync_calls: HashMap<u16, oneshot::Sender<Result<()>>>,
    channels: HashMap<u16, Channel>,
    in_delivery: HashMap<Channel, DeliveredContent>,
}

/// Create a connection to an AMQP server and returns a sink to send the requests.
async fn create_connection(url: &str) -> Result<ClientRequestSink> {
    use tokio::net::TcpStream;

    match TcpStream::connect(url).await {
        Ok(socket) => {
            let (sender, receiver) = mpsc::channel(1);

            tokio::spawn(async move {
                if let Err(e) = processor::start_loop_and_output(socket, receiver).await {
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
        //let mut rng = rand::thread_rng();

        let (connected_tx, connected_rx) = oneshot::channel();
        let client_sink = create_connection(url).await?;

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
            sync_calls: HashMap::new(),
            channels: HashMap::new(),
            in_delivery: HashMap::new(),
        })
    }

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
}
