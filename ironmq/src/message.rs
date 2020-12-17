//! Messages are sent to exhchanges and forwarded to queues. There is a
//! possibility to state that a message is processed via an oneshot channel.
use tokio::sync::oneshot;

pub(crate) type MessageId = String;

#[derive(Debug)]
pub(crate) struct Message {
    pub(crate) content: Vec<u8>,
    pub(crate) processed: Option<oneshot::Sender<MessageId>>,
}
