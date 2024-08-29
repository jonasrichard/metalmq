use std::collections::HashMap;

use crate::exchange;
use crate::queue;
use crate::Result;

use metalmq_codec::codec::Frame;
use metalmq_codec::frame::{AMQPFrame, ContentBodyFrame, ContentHeaderFrame};

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

#[derive(Debug)]
pub enum ChannelError {
    ContentTooLarge = 311,
    NoRoute = 312,
    NoConsumers = 313,
    AccessRefused = 403,
    NotFound = 404,
    ResourceLocked = 405,
    PreconditionFailed = 406,
}

/// Queues consumed by the connection with Basic.Consume
#[derive(Debug)]
pub struct ActivelyConsumedQueue {
    pub queue_name: String,
    pub consumer_tag: String,
    pub queue_sink: queue::handler::QueueCommandSink,
}

/// Queues consumed by the connection with Basic.Get
#[derive(Debug)]
pub struct PassivelyConsumedQueue {
    pub queue_name: String,
    pub consumer_tag: String,
    pub delivery_tag: u64,
    pub queue_sink: queue::handler::QueueCommandSink,
}

#[derive(Debug, Default)]
pub struct PublishedContent {
    pub source_connection: String,
    pub channel: u16,
    pub exchange: String,
    pub routing_key: String,
    pub mandatory: bool,
    pub immediate: bool,
    /// The method frame class id which initiated the sending of the content.
    pub method_frame_class_id: u32,
    pub content_header: ContentHeaderFrame,
    pub content_bodies: Vec<ContentBodyFrame>,
    pub body_size: usize,
}

/// Represents a channel
#[derive(Debug)]
pub struct Channel {
    pub source_connection: String,
    pub number: u16,
    pub consumed_queue: Option<ActivelyConsumedQueue>,
    pub in_flight_content: Option<PublishedContent>,
    pub confirm_mode: bool,
    pub next_confirm_delivery_tag: u64,
    pub outgoing: mpsc::Sender<Frame>,
    pub exchanges: HashMap<String, exchange::handler::ExchangeCommandSink>,
}

impl Channel {
    pub async fn start(
        connection_id: String,
        channel_number: u16,
        outgoing: mpsc::Sender<Frame>,
    ) -> (mpsc::Sender<AMQPFrame>, JoinHandle<Result<()>>) {
        let mut channel = Channel {
            source_connection: connection_id,
            number: channel_number,
            consumed_queue: None,
            in_flight_content: None,
            confirm_mode: false,
            next_confirm_delivery_tag: 1u64,
            outgoing,
            exchanges: HashMap::new(),
        };

        let (tx, rx) = mpsc::channel(16);

        let jh = tokio::spawn(async move { channel.handle_message(rx).await });

        (tx, jh)
    }

    pub async fn handle_message(&mut self, mut rx: mpsc::Receiver<AMQPFrame>) -> Result<()> {
        while let Some(f) = rx.recv().await {}

        Ok(())
    }
}
