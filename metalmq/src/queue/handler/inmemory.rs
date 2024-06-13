use std::collections::VecDeque;

use super::{DeliveredMessage, Outbox};

pub struct MessageQueue {
    messages: VecDeque<DeliveredMessage>,
    outbox: Outbox,
}

impl MessageQueue {
    // TODO
    // Here we need to pass the list of consumers because
    // - there can be messages rejected by some consumers and we need to provide them to other
    // consumers
    // - consumers can use some qos with prefetch, so this function will distribute messages
    // according to that. If someone exceeeded the prefetch limit, it shouldn't get more messages
    // but others can. So we need to maintain how many messages have been sent out without being
    // acked.
    pub fn take_message(&mut self, _connections: Vec<String>) -> Option<DeliveredMessage> {
        self.messages.pop_front()
    }
}
