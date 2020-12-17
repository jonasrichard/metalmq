use crate::message::Message;
use std::collections::VecDeque;

pub(crate) struct Queue {
    name: String,
    // TODO I am not sure that we need the message with the ready channel here
    messages: VecDeque<Message>,
}
