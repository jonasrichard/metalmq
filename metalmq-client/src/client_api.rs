use crate::channel_api::{Channel, ConsumerSignal, DeliveredContent};
use crate::client_error;
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
    Start(String, String),
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
            Param::Start(user, _) => write!(f, "Start{{Username={:?}}}", user),
            Param::Frame(frame) => write!(f, "Request{{Frame={:?}}}", frame),
            Param::Consume(frame, _) => write!(f, "Request{{Consume={:?}}}", frame),
            Param::Publish(frame, _) => write!(f, "Request{{Publish={:?}}}", frame),
        }
    }
}

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

// TODO expect only one parameter and parse the url
pub async fn connect(url: &str, username: &str, password: &str) -> Result<Client> {
    //let mut rng = rand::thread_rng();

    // but it needs to do the authentication and to connect to virtualhost
    let client_sink = create_connection(url).await?;

    processor::call(&client_sink, frame::AMQPFrame::Header).await?;

    let mut caps = frame::FieldTable::new();

    caps.insert(
        "authentication_failure_close".to_string(),
        frame::AMQPFieldValue::Bool(true),
    );

    //caps.insert("basic.nack".to_string(), AMQPFieldValue::Bool(true));
    //caps.insert("connection.blocked".to_string(), AMQPFieldValue::Bool(true));
    //caps.insert("consumer_cancel_notify".to_string(), AMQPFieldValue::Bool(true));
    //caps.insert("publisher_confirms".to_string(), AMQPFieldValue::Bool(true));

    processor::call(&client_sink, frame::connection_start_ok(username, password, caps)).await?;

    processor::send(&client_sink, frame::connection_tune_ok(0)).await?;

    processor::call(&client_sink, frame::connection_open(0, "/")).await?;

    Ok(Client {
        connection_id: "01234".to_owned(),
        request_sink: client_sink,
        sync_calls: HashMap::new(),
        channels: HashMap::new(),
        in_delivery: HashMap::new(),
    })
}

/// Create a connection to an AMQP server and returns a sink to send the requests.
async fn create_connection(url: &str) -> Result<ClientRequestSink> {
    use tokio::net::TcpStream;

    match TcpStream::connect(url).await {
        Ok(socket) => {
            let (sender, receiver) = mpsc::channel(1);

            tokio::spawn(async move {
                if let Err(e) = processor::socket_loop(socket, receiver).await {
                    error!("error: {:?}", e);
                }
            });

            Ok(sender)
        }
        Err(e) => Err(anyhow!("Connection error {:?}", e)),
    }
}

impl Client {
    pub async fn channel_open(&mut self, channel: ChannelNumber) -> Result<Channel> {
        processor::call(&self.request_sink, frame::channel_open(channel)).await?;

        Ok(Channel::new(channel, self.request_sink.clone()))
    }

    pub async fn close(&mut self) -> Result<()> {
        let fr = frame::connection_close(0, 200, "Normal close", 0, 0);

        processor::call(&self.request_sink, fr).await?;

        Ok(())
    }
}
