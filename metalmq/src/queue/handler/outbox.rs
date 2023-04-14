use std::time::Instant;

use super::{DeliveredMessage, Tag};

#[derive(Debug)]
pub struct OutgoingMessage {
    pub message: DeliveredMessage,
    pub tag: Tag,
    pub sent_at: Instant,
}

#[derive(Debug)]
pub struct Outbox {
    pub outgoing_messages: Vec<OutgoingMessage>,
}

impl Outbox {
    /// Acking messages by remove them based on the parameters. Return false if a message is double
    /// acked.
    pub fn on_ack_arrive(&mut self, consumer_tag: String, delivery_tag: u64, multiple: bool) -> bool {
        match (multiple, delivery_tag) {
            (true, 0) => {
                // If multiple is true and delivery tag is 0, we ack all sent messages with that
                // consumer tag.
                self.outgoing_messages.retain(|om| om.tag.consumer_tag != consumer_tag);

                true
            }
            (true, _) => {
                // If multiple is true and delivery tag is non-zero, we ack all sent messages with
                // delivery tag less than equal with that consumer tag.
                self.outgoing_messages
                    .retain(|om| om.tag.consumer_tag != consumer_tag || om.tag.delivery_tag > delivery_tag);

                true
            }
            (false, _) => {
                // If multiple is false, we ack the sent out message with that consumer tag and
                // delivery tag.
                dbg!(&self.outgoing_messages);
                dbg!(&consumer_tag);
                match self
                    .outgoing_messages
                    .iter()
                    .position(|om| om.tag.consumer_tag == consumer_tag && om.tag.delivery_tag == delivery_tag)
                {
                    None => false,
                    Some(p) => {
                        self.outgoing_messages.remove(p);
                        true
                    }
                }
            }
        }
    }

    pub fn on_sent_out(&mut self, outgoing_message: OutgoingMessage) {
        self.outgoing_messages.push(outgoing_message);
    }

    pub fn remove_messages_by_ctag(&mut self, ctag: &str) -> Vec<DeliveredMessage> {
        let mut messages = vec![];
        let mut i = 0;

        while i < self.outgoing_messages.len() {
            if self.outgoing_messages[i].tag.consumer_tag == ctag {
                messages.push(self.outgoing_messages.remove(i).message);
            } else {
                i += 1;
            }
        }

        messages
    }
}
