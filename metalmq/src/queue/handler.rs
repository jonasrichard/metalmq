use crate::message::{self, Message};
use crate::{ConsumerTag, Result};
use log::{debug, error, info, warn};
use metalmq_codec::frame;
use std::collections::VecDeque;
use tokio::sync::{mpsc, oneshot};

pub(crate) type QueueCommandSink = mpsc::Sender<QueueCommand>;
pub(crate) type FrameSink = mpsc::Sender<frame::AMQPFrame>;
//pub(crate) type FrameStream = mpsc::Receiver<frame::AMQPFrame>;

/// Information about the queue instance
pub(crate) struct QueueInfo {
    pub name: String,
    // TODO message metrics, current, current outgoing, etc...
}

#[derive(Debug)]
pub(crate) enum QueueCommand {
    Message(Message),
    Ack {
        consumer_tag: String,
        delivery_tag: u64,
    },
    Consume {
        consumer_tag: String,
        no_ack: bool,
        frame_sink: FrameSink,
        response: oneshot::Sender<()>,
    },
    Cancel {
        consumer_tag: String,
        response: oneshot::Sender<()>,
    },
}

struct Consumer {
    /// Consumer tag, identifies the consumer
    consumer_tag: String,
    /// Consumer doesn't need ack, so we can delete sent-out messages promptly
    no_ack: bool,
    /// Consumer network socket abstraction
    sink: FrameSink,
    /// The next delivery tag it needs to send out
    delivery_tag_counter: u64,
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
pub(crate) async fn queue_loop(commands: &mut mpsc::Receiver<QueueCommand>) {
    // TODO we need to have a variable here to access the queue properties
    let mut messages = VecDeque::<Message>::new();
    let mut outbox = Outbox {
        outgoing_messages: vec![],
    };
    let mut consumers = Vec::<Consumer>::new();
    let mut current_consumer = 0;

    // TODO we need to store the delivery tags by consumers
    // Also we need to mark a message that it is sent, so we need to wait
    // for the ack, until that we cannot send new messages out - or depending
    // the consuming yes?

    while let Some(command) = commands.recv().await {
        match command {
            QueueCommand::Message(message) => {
                debug!("Sending message to consumers {}", consumers.len());

                let mut has_sent = false;
                let mut no_ack: Option<bool> = None;
                let mut actual_consumer = current_consumer;
                // TODO collect the indices what we need to remove, this is not optimal
                let mut failed = Vec::<usize>::new();

                while let Some(consumer) = consumers.get_mut(actual_consumer) {
                    let frames = message_to_frames(&message, &consumer.consumer_tag, consumer.delivery_tag_counter);

                    consumer.delivery_tag_counter += 1;

                    if let Ok(true) = send_message(&consumer.sink, frames).await {
                        has_sent = true;
                        no_ack = Some(consumer.no_ack);

                        break;
                    } else {
                        warn!(
                            "Error sending message to consumer with tag {}, removing",
                            consumer.consumer_tag
                        );
                        failed.push(actual_consumer);
                    }

                    actual_consumer = (actual_consumer + 1) % consumers.len();
                    if actual_consumer == current_consumer {
                        break;
                    }
                }

                if failed.len() > 0 {
                    current_consumer = 0;

                    for f in failed.iter().rev() {
                        consumers.remove(*f);
                    }
                }

                if !has_sent {
                    messages.push_back(message);
                } else {
                    if let Some(false) = no_ack {
                        outbox.on_sent_out(OutgoingMessage {
                            message,
                            consumer_tag: "".to_string(),
                            delivery_tag: 0u64,
                        });
                    }
                }
            }
            QueueCommand::Consume {
                consumer_tag,
                no_ack,
                frame_sink,
                response,
            } => {
                info!("Basic Consume {}", consumer_tag);

                if let Err(e) = response.send(()) {
                    error!("Send error {:?}", e);
                }

                if messages.len() > 0 {
                    info!("Send messages to new consumer");
                }

                let mut delivery_tag_counter = 1u64;

                // TODO now we send the first consumer all the messages but it is not optimal
                // we need to send messages other consumers as well, if they are appearing
                'send: while let Some(message) = messages.pop_front() {
                    let frames = message_to_frames(&message, &consumer_tag, delivery_tag_counter);
                    delivery_tag_counter += 1;

                    if let Ok(false) = send_message(&frame_sink, frames).await {
                        error!("Cannot send message to a consumer {}", consumer_tag);
                        break 'send;
                    }
                }

                consumers.push(Consumer {
                    consumer_tag,
                    no_ack,
                    sink: frame_sink,
                    delivery_tag_counter,
                });
            }
            QueueCommand::Cancel { consumer_tag, response } => {
                info!("Basic Cancel {}", consumer_tag);

                consumers
                    .binary_search_by(|c| c.consumer_tag.cmp(&consumer_tag))
                    .map(|pos| consumers.remove(pos))
                    .map_err(|e| error!("Cannot cancel consumer {:?}", e));

                if let Err(e) = response.send(()) {
                    error!("Send error {:?}", e);
                }
            }
            QueueCommand::Ack {
                consumer_tag,
                delivery_tag,
            } => {
                warn!("TODO implement ack for {} and {}", consumer_tag, delivery_tag);

                outbox.on_ack_arrive(consumer_tag, delivery_tag);
            }
        }
    }
}

/// Send the message as frames to a consumer. Returns true if all the frames managed to
/// be sent to the channel. The caller of this function should take care of the result,
/// and in case of a failed sending, it should try to send to another consumer, or
/// if there is no consumer, it should store the message.
async fn send_message(consumer: &FrameSink, frames: Vec<frame::AMQPFrame>) -> Result<bool> {
    let mut n: usize = 0;

    'frames: for f in &frames {
        debug!("Sending frame {:?}", f);

        if let Err(e) = consumer.send(f.clone()).await {
            // TODO remove this channel from the consumers
            error!("Message send error {:?}", e);
            break 'frames;
        } else {
            n += 1
        }
    }

    Ok(n == frames.len())
}

fn message_to_frames(message: &Message, consumer_tag: &ConsumerTag, delivery_tag: u64) -> Vec<frame::AMQPFrame> {
    let mut frames = message::message_to_content_frames(&message);

    let basic_deliver = frame::basic_deliver(
        message.channel,
        consumer_tag,
        delivery_tag,
        false,
        &message.exchange,
        &message.routing_key,
    );
    frames.insert(0, basic_deliver);

    frames
}

struct OutgoingMessage {
    message: Message,
    consumer_tag: String,
    delivery_tag: u64,
}

struct Outbox {
    outgoing_messages: Vec<OutgoingMessage>,
}

impl Outbox {
    fn on_ack_arrive(&mut self, consumer_tag: String, delivery_tag: u64) {
        self.outgoing_messages.retain(|om| {
            om.delivery_tag != delivery_tag || om.consumer_tag.cmp(&consumer_tag) != std::cmp::Ordering::Equal
        });
    }

    fn on_sent_out(&mut self, outgoing_message: OutgoingMessage) {
        self.outgoing_messages.push(outgoing_message);
    }
}
