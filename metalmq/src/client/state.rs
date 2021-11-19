// Do we need to expose the messages of a 'process' or hide it in an erlang-style?
use crate::exchange::handler::ExchangeCommandSink;
use crate::exchange::manager as em;
use crate::queue::handler as queue_handler;
use crate::queue::manager as qm;
use crate::{Context, Result};
use log::info;
use metalmq_codec::codec::Frame;
use metalmq_codec::frame::Channel;
use std::collections::HashMap;
use tokio::sync::mpsc;
use uuid::Uuid;

pub mod basic;
pub mod channel;
pub mod connect;
pub mod exchange;
pub mod queue;

pub type MaybeFrame = Result<Option<Frame>>;

#[derive(Debug)]
struct ConsumedQueue {
    channel: Channel,
    queue_name: String,
    consumer_tag: String,
    queue_sink: queue_handler::QueueCommandSink,
}

/// All the transient data of a connection are stored here.
pub struct Connection {
    /// Unique ID of the connection.
    id: String,
    qm: qm::QueueManagerSink,
    em: em::ExchangeManagerSink,
    /// Opened channels by this connection.
    open_channels: Vec<Channel>,
    /// Declared exchanges by this connection.
    exchanges: HashMap<String, ExchangeCommandSink>,
    /// Exchanges which declared by this channel as auto-delete
    auto_delete_exchanges: Vec<String>,
    /// Consumed queues by this connection
    consumed_queues: Vec<ConsumedQueue>,
    /// Incoming messages come in different messages, we need to collect their properties
    in_flight_contents: HashMap<Channel, PublishedContent>,
    /// Sink for AMQP frames toward the client
    outgoing: mpsc::Sender<Frame>,
}

#[derive(Debug, Default)]
struct PublishedContent {
    channel: Channel,
    exchange: String,
    routing_key: String,
    mandatory: bool,
    immediate: bool,
    content_type: Option<String>,
    length: Option<u64>,
    content: Option<Vec<u8>>,
}

pub(crate) fn new(context: Context, outgoing: mpsc::Sender<Frame>) -> Connection {
    let conn_id = Uuid::new_v4().to_hyphenated().to_string();

    info!("Client connected id = {}", conn_id);

    Connection {
        id: conn_id,
        qm: context.queue_manager,
        em: context.exchange_manager,
        open_channels: vec![],
        exchanges: HashMap::new(),
        auto_delete_exchanges: vec![],
        consumed_queues: vec![],
        in_flight_contents: HashMap::new(),
        outgoing,
    }
}
