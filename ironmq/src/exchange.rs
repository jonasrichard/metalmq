//! Exchanges Handle incoming messages and forward them to other exhanges
//! or queues. Each exchange is a lightweight process and handle messages
//! through a channel. When a client is publishing to an exchange it should
//! clone the exchange channel, so the messages will be handled serially.
use crate::message;
use tokio::sync::{mpsc, oneshot};

pub(crate) struct IncomingMessage {
    message: message::Message,
    ready: oneshot::Sender<()>
}

pub(crate) struct Exchange {
    input: mpsc::Sender<IncomingMessage>
}
