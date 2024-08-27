use std::collections::HashMap;

use metalmq_codec::{codec::Frame, frame::AMQPFrame};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::Result;

pub struct Connection {
    /// Unique ID of the connection.
    pub id: String,
    /// The highest channel number, 0 if there is no limit.
    pub channel_max: u16,
    /// The maximal size of frames the client can accept.
    pub frame_max: usize,
    /// How frequently the server sends heartbeat (at most).
    pub heartbeat_interval: Option<std::time::Duration>,
    pub channel_handlers: HashMap<u16, JoinHandle<Result<()>>>,
    pub channel_receivers: HashMap<u16, mpsc::Sender<AMQPFrame>>,
    /// Sink for AMQP frames toward the client
    pub outgoing: mpsc::Sender<Frame>,
}
