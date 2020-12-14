use crate::exchange::IncomingMessage;
use std::collections::VecDeque;

pub(crate) struct Queue {
    name: String,
    messages: VecDeque<IncomingMessage>,
}
