use std::collections::HashMap;

use log::info;
use metalmq_codec::{codec::Frame, frame::AMQPFrame};
use tokio::{sync::mpsc, task::JoinHandle};
use uuid::Uuid;

use crate::{exchange, queue, Context, Result};

/// Exclusive queue declared by the connection
#[derive(Debug)]
pub struct ExclusiveQueue {
    pub queue_name: String,
}

pub struct Connection {
    /// Unique ID of the connection.
    pub id: String,
    /// The highest channel number, 0 if there is no limit.
    pub channel_max: u16,
    /// The maximal size of frames the client can accept.
    pub frame_max: usize,
    /// How frequently the server sends heartbeat (at most).
    pub heartbeat_interval: Option<std::time::Duration>,
    /// Exchanges which declared by this channel as auto-delete
    pub auto_delete_exchanges: Vec<String>,
    /// Exclusive queues created by the connection.
    pub exclusive_queues: Vec<ExclusiveQueue>,
    pub qm: queue::manager::QueueManagerSink,
    pub em: exchange::manager::ExchangeManagerSink,
    pub channel_handlers: HashMap<u16, JoinHandle<Result<()>>>,
    pub channel_receivers: HashMap<u16, mpsc::Sender<AMQPFrame>>,
    /// Sink for AMQP frames toward the client
    pub outgoing: mpsc::Sender<Frame>,
}

impl Connection {
    pub fn new(context: Context, outgoing: mpsc::Sender<Frame>) -> Self {
        let conn_id = Uuid::new_v4().as_hyphenated().to_string();

        info!("Client connected id = {conn_id}");

        Self {
            id: conn_id,
            qm: context.queue_manager,
            em: context.exchange_manager,
            channel_max: 2047,
            frame_max: 131_072,
            heartbeat_interval: None,
            auto_delete_exchanges: vec![],
            exclusive_queues: vec![],
            channel_handlers: HashMap::new(),
            channel_receivers: HashMap::new(),
            outgoing,
        }
    }
}
