use std::collections::HashMap;

use crate::exchange;
use crate::queue;

use metalmq_codec::codec::Frame;
use metalmq_codec::frame::MethodFrameArgs;
use metalmq_codec::frame::{ContentBodyFrame, ContentHeaderFrame};

use tokio::sync::mpsc;

#[derive(Debug)]
pub enum Command {
    MethodFrame(u16, u32, MethodFrameArgs),
    Close(u16, u32, String),
    ContentHeader(ContentHeaderFrame),
    ContentBody(ContentBodyFrame),
}

/// Queues consumed by the connection with Basic.Consume
#[derive(Debug)]
pub struct ActivelyConsumedQueue {
    pub queue_name: String,
    pub consumer_tag: String,
    pub queue_sink: queue::handler::QueueCommandSink,
}

/// Queues consumed by the connection with Basic.Get
#[derive(Clone, Debug)]
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
    pub passively_consumed_queue: Option<PassivelyConsumedQueue>,
    pub in_flight_content: Option<PublishedContent>,
    pub confirm_mode: bool,
    pub next_confirm_delivery_tag: Option<u64>,
    pub frame_size: usize,
    pub outgoing: mpsc::Sender<Frame>,
    pub exchanges: HashMap<String, exchange::handler::ExchangeCommandSink>,
    pub em: exchange::manager::ExchangeManagerSink,
    pub qm: queue::manager::QueueManagerSink,
}
