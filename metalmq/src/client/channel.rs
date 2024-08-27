use metalmq_codec::{
    codec::Frame,
    frame::{self, AMQPFrame, ContentBodyFrame, ContentHeaderFrame},
};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{queue::handler as queue_handler, ErrorScope, Result, RuntimeError};

pub mod basic;
pub mod exchange;
pub mod open_close;
pub mod queue;

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
    pub queue_sink: queue_handler::QueueCommandSink,
}

/// Queues consumed by the connection with Basic.Get
#[derive(Debug)]
pub struct PassivelyConsumedQueue {
    pub queue_name: String,
    pub consumer_tag: String,
    pub delivery_tag: u64,
    pub queue_sink: queue_handler::QueueCommandSink,
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
    pub number: u16,
    pub consumed_queue: Option<ActivelyConsumedQueue>,
    pub in_flight_content: Option<PublishedContent>,
    pub confirm_mode: bool,
    pub next_confirm_delivery_tag: u64,
    pub outgoing: mpsc::Sender<Frame>,
}

/// Helper to create channel error frames.
pub fn channel_error<T>(channel: u16, cm: u32, code: ChannelError, text: &str) -> Result<T> {
    Err(Box::new(RuntimeError {
        scope: ErrorScope::Channel,
        channel,
        code: code as u16,
        text: text.to_owned(),
        class_method: cm,
    }))
}

// TODO move all error converstion to an error mod
pub fn runtime_error_to_frame(rte: &RuntimeError) -> Frame {
    let amqp_frame = match rte.scope {
        ErrorScope::Connection => frame::connection_close(rte.code, &rte.text, rte.class_method),
        ErrorScope::Channel => frame::channel_close(rte.channel, rte.code, &rte.text, rte.class_method),
    };

    Frame::Frame(amqp_frame)
}

impl Channel {
    pub async fn start(
        channel_number: u16,
        outgoing: mpsc::Sender<Frame>,
    ) -> (mpsc::Sender<AMQPFrame>, JoinHandle<Result<()>>) {
        let mut channel = Channel {
            number: channel_number,
            consumed_queue: None,
            in_flight_content: None,
            confirm_mode: false,
            next_confirm_delivery_tag: 1u64,
            outgoing,
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
