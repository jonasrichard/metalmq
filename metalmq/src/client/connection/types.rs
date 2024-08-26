use std::collections::HashMap;

use metalmq_codec::{codec::Frame, frame::AMQPFrame};
use tokio::{sync::mpsc, task::JoinHandle};

pub struct Connection {
    pub channel_handlers: HashMap<u16, JoinHandle<()>>,
    pub channel_receivers: HashMap<u16, mpsc::Sender<AMQPFrame>>,
    /// Sink for AMQP frames toward the client
    pub outgoing: mpsc::Sender<Frame>,
}
