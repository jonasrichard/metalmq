use crate::message::Message;
use crate::queue::consumer_handler::{ConsumerCommand, ConsumerCommandSink};
use log::{error, trace};
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

#[derive(Debug)]
pub(crate) enum QueueCommand {
    PublishMessage(Message),
    GetMessage {
        tags: Option<(String, u64)>,
        // We need to generate a message id not to look for consumer tag, delivery tag
        result: oneshot::Sender<Option<Message>>,
    },
    AckMessage {
        consumer_tag: String,
        delivery_tag: u64,
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

                if let Err(e) = consumer_sink.send(ConsumerCommand::MessagePublished).await {
                    error!("Error {:?}", e);
                };
            }
            QueueCommand::GetMessage { result, tags } => {
                trace!("Get message from queue");

                if let Some(message) = messages.pop_front() {
                    trace!("Giving to consumer handler {:?}", message);

                    outbox.on_sent_out(OutgoingMessage {
                        message: message.clone(),
                        tags,
                        sent_at: Instant::now(),
                    });

                    if let Err(e) = result.send(Some(message)) {
                        error!("Error {:?}", e);
                    }
                } else {
                    if let Err(e) = result.send(None) {
                        error!("Error {:?}", e);
                    }
                }
            }
            QueueCommand::AckMessage {
                consumer_tag,
                delivery_tag,
            } => {
                outbox.on_ack_arrive(consumer_tag, delivery_tag);
            }
        }
    }
}

struct OutgoingMessage {
    message: Message,
    tags: Option<(String, u64)>,
    sent_at: Instant,
}

struct Outbox {
    outgoing_messages: Vec<OutgoingMessage>,
}

impl Outbox {
    fn on_ack_arrive(&mut self, consumer_tag: String, delivery_tag: u64) {
        self.outgoing_messages.retain(|om| {
            if let Some((ctag, dtag)) = &om.tags {
                dtag != &delivery_tag || ctag.cmp(&consumer_tag) != std::cmp::Ordering::Equal
            } else {
                true
            }
        });
    }

    fn on_sent_out(&mut self, outgoing_message: OutgoingMessage) {
        self.outgoing_messages.push(outgoing_message);
    }
}
