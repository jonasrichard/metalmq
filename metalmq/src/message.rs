//! Messages are sent to exhchanges and forwarded to queues. There is a
//! possibility to state that a message is processed via an oneshot channel.
use tokio::sync::mpsc;

//pub(crate) type MessageId = String;

#[derive(Clone, Debug)]
pub(crate) struct Message {
    /// Id of the connection sent this message.
    pub(crate) source_connection: String,
    pub(crate) channel: u16, // TODO use channel type here
    pub(crate) content: Vec<u8>,
    pub(crate) exchange: String,
    pub(crate) routing_key: String,
    pub(crate) mandatory: bool,
    pub(crate) immediate: bool,
}

pub(crate) type MessageChannel = mpsc::Sender<Message>;
