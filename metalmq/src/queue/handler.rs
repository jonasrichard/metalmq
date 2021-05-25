use crate::message::{self, Message};
use crate::{ConsumerTag, Result};
use log::{debug, error, info, warn};
use metalmq_codec::frame;
use std::collections::VecDeque;
use tokio::sync::{mpsc, oneshot};

pub(crate) type QueueCommandSink = mpsc::Sender<QueueCommand>;
pub(crate) type FrameSink = mpsc::Sender<frame::AMQPFrame>;
//pub(crate) type FrameStream = mpsc::Receiver<frame::AMQPFrame>;

#[derive(Debug)]
pub(crate) enum QueueCommand {
    Message(Message),
    Ack {
        consumer_tag: String,
        delivery_tag: u64,
    },
    Consume {
        consumer_tag: String,
        frame_sink: FrameSink,
        response: oneshot::Sender<()>,
    },
    Cancel {
        consumer_tag: String,
        response: oneshot::Sender<()>,
    },
}

struct Consumer {
    consumer_tag: String,
    sink: FrameSink,
}

pub(crate) async fn queue_loop(commands: &mut mpsc::Receiver<QueueCommand>) {
    // TODO we need to have a variable here to access the queue properties
    let mut messages = VecDeque::<Message>::new();
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
                let mut actual_consumer = current_consumer;
                // TODO collect the indices what we need to remove, this is not optimal
                let mut failed = Vec::<usize>::new();

                while let Some(consumer) = consumers.get(actual_consumer) {
                    let frames = message_to_frames(&message, &consumer.consumer_tag);

                    if let Ok(true) = send_message(&consumer.sink, frames).await {
                        has_sent = true;

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
                }
            }
            QueueCommand::Consume {
                consumer_tag,
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

                // TODO now we send the first consumer all the messages but it is not optimal
                // we need to send messages other consumers as well, if they are appearing
                'send: while let Some(message) = messages.pop_front() {
                    let frames = message_to_frames(&message, &consumer_tag);

                    if let Ok(false) = send_message(&frame_sink, frames).await {
                        error!("Cannot send message to a consumer {}", consumer_tag);
                        break 'send;
                    }
                }

                consumers.push(Consumer {
                    consumer_tag,
                    sink: frame_sink,
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
                // TODO we need to remove the messages with that consumer and delivery tag
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

fn message_to_frames(message: &Message, consumer_tag: &ConsumerTag) -> Vec<frame::AMQPFrame> {
    let mut frames = message::message_to_content_frames(&message);

    let basic_deliver = frame::basic_deliver(
        message.channel,
        consumer_tag,
        0,
        false,
        &message.exchange,
        &message.routing_key,
    );
    frames.insert(0, basic_deliver);

    frames
}
