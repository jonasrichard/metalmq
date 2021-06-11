use crate::logerr;
use crate::message::{self, Message};
use crate::queue::consumer_handler::{ConsumerCommand, ConsumerCommandSink, FrameSink, SendResult};
use log::{error, info, trace};
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot};

pub(crate) type QueueCommandSink = mpsc::Sender<QueueCommand>;
//pub(crate) type FrameStream = mpsc::Receiver<frame::AMQPFrame>;

/// Information about the queue instance
pub(crate) struct QueueInfo {
    pub name: String,
    // TODO message metrics, current, current outgoing, etc...
}

#[derive(Clone, Debug)]
pub(crate) struct Tag {
    pub consumer_tag: String,
    pub delivery_tag: u64,
}

#[derive(Debug)]
pub(crate) enum QueueCommand {
    PublishMessage(Message),
    DeliverMessage {
        tag: Tag,
        outgoing: FrameSink,
        result: oneshot::Sender<SendResult>,
    },
    AckMessage {
        consumer_tag: String,
        delivery_tag: u64,
    },
    ExchangeBound {
        exchange_name: String,
    },
    ExchangeUnbound {
        exchange_name: String,
    },
}

// Message delivery
//   pick up a consumer - randomly
//   pick up the next message from the queue
//   send it via the channel and
//     mark it as SentOut
//     store the timestamp
//     set delivery try = 1
//   send multiple messages like 10 in a way - max out flight messages
//   if a messages is acked, let us remove from the queue
//   since we set up a time, if there is a timeout, we can redeliver the message
//     (it would be good to choose a different consumer)
//     set delivery try += 1
//     if delivery try is greater than 5 before, we can drop the message
//       (later we can send it to an alternative queue)
//
//  Cancel consume
//    All messages which are outflight needs to be redelivered to the
//      remaining consumers.
pub(crate) async fn queue_loop(commands: &mut mpsc::Receiver<QueueCommand>, consumer_sink: ConsumerCommandSink) {
    // TODO we need to have a variable here to access the queue properties
    let mut messages = VecDeque::<Message>::new();
    let mut outbox = Outbox {
        outgoing_messages: vec![],
    };

    // TODO we need to store the delivery tags by consumers
    // Also we need to mark a message that it is sent, so we need to wait
    // for the ack, until that we cannot send new messages out - or depending
    // the consuming yes?

    while let Some(command) = commands.recv().await {
        match command {
            QueueCommand::PublishMessage(message) => {
                trace!("Queue message {:?}", message);

                messages.push_back(message);

                // TODO this only we need to do if there are consumers, otherwise they will
                // get the message and drop that, if so... we can even send over the message
                // itself
                if let Err(e) = consumer_sink.send(ConsumerCommand::MessagePublished).await {
                    error!("Error {:?}", e);
                };
            }
            QueueCommand::DeliverMessage { tag, outgoing, result } => {
                if let Some(message) = messages.pop_front() {
                    outbox.on_sent_out(OutgoingMessage {
                        message: message.clone(),
                        tag: tag.clone(),
                        sent_at: Instant::now(),
                    });

                    match message::send_message(&message, &tag, outgoing).await {
                        Ok(()) => {
                            logerr!(result.send(SendResult::MessageSent));
                        }
                        Err(e) => {
                            error!("Error sending out message {:?}", e);

                            logerr!(result.send(SendResult::ConsumerInvalid));
                        }
                    }
                } else {
                    logerr!(result.send(SendResult::QueueEmpty));
                }
            }
            QueueCommand::AckMessage {
                consumer_tag,
                delivery_tag,
            } => {
                outbox.on_ack_arrive(consumer_tag, delivery_tag);
            }
            QueueCommand::ExchangeBound { exchange_name } => {
                info!("Unbound exchange {}", exchange_name);
            }
            QueueCommand::ExchangeUnbound { exchange_name } => {
                info!("Unbound exchange {}", exchange_name);
            }
        }
    }
}

struct OutgoingMessage {
    // TODO we don't need to store the whole message but rather the id
    message: Message,
    tag: Tag,
    sent_at: Instant,
}

struct Outbox {
    outgoing_messages: Vec<OutgoingMessage>,
}

impl Outbox {
    fn on_ack_arrive(&mut self, consumer_tag: String, delivery_tag: u64) {
        self.outgoing_messages.retain(|om| {
            &om.tag.delivery_tag != &delivery_tag || om.tag.consumer_tag.cmp(&consumer_tag) != Ordering::Equal
        });
    }

    fn on_sent_out(&mut self, outgoing_message: OutgoingMessage) {
        self.outgoing_messages.push(outgoing_message);
    }
}
