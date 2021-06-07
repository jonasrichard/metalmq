use crate::message::{self, Message};
use crate::queue::handler::{QueueCommand, Tag};
use crate::Result;
use log::{debug, error, info};
use metalmq_codec::frame::{self, AMQPFrame};
use std::cmp::Ordering;
use tokio::sync::{mpsc, oneshot};
use tokio::time;

#[derive(Debug)]
pub(crate) enum ConsumerCommand {
    StartConsuming {
        consumer_tag: String,
        no_ack: bool,
        sink: FrameSink,
        result: oneshot::Sender<()>,
    },
    CancelConsuming {
        consumer_tag: String,
        result: oneshot::Sender<()>,
    },
    MessagePublished,
    MessageAcked {
        consumer_tag: String,
        delivery_tag: u64,
    },
    MessageRejected,
    Recover,
}

pub(crate) type ConsumerCommandSink = mpsc::Sender<ConsumerCommand>;
pub(crate) type FrameSink = mpsc::Sender<AMQPFrame>;

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

pub(crate) async fn consumer_handler_loop(
    commands: &mut mpsc::Receiver<ConsumerCommand>,
    queue: mpsc::Sender<QueueCommand>,
) {
    let mut consumers = Vec::<Consumer>::new();

    while let Some(command) = commands.recv().await {
        match command {
            ConsumerCommand::StartConsuming {
                consumer_tag,
                no_ack,
                sink,
                result,
            } => {
                info!("Start consuming tag {} no_ack {}", consumer_tag, no_ack);

                if let Err(e) = result.send(()) {
                    error!("Error {:?}", e);
                }

                let mut delivery_tag = 1u64;

                'consumer: loop {
                    let tag = Tag {
                        consumer_tag: consumer_tag.clone(),
                        delivery_tag,
                    };

                    match get_and_send_message(&queue, &sink, tag).await {
                        Ok(SendResult::MessageSent) => {
                            delivery_tag += 1;
                        }
                        Ok(SendResult::QueueEmpty) => {
                            consumers.push(Consumer {
                                consumer_tag: consumer_tag.clone(),
                                no_ack,
                                sink,
                                delivery_tag_counter: delivery_tag,
                            });
                            break 'consumer;
                        }
                        Ok(SendResult::ConsumerInvalid) => {
                            break 'consumer;
                        }
                        Err(e) => error!("Error {:?}", e),
                    }
                }
            }
            ConsumerCommand::CancelConsuming { consumer_tag, result } => {
                info!("Cancel consuming tag {}", consumer_tag);

                consumers.retain(|c| c.consumer_tag.cmp(&consumer_tag) == Ordering::Equal);

                result.send(());
            }
            ConsumerCommand::MessagePublished => {
                if let Some(consumer) = consumers.first_mut() {
                    if let Err(e) = get_message_from_queue_and_send(consumer, &queue).await {
                        error!("Error {:?}", e);
                    }
                }
            }
            _ => (),
        }
    }
}

enum SendResult {
    MessageSent,
    QueueEmpty,
    ConsumerInvalid,
}

async fn get_and_send_message(queue: &mpsc::Sender<QueueCommand>, sink: &FrameSink, tag: Tag) -> Result<SendResult> {
    let delivery_tag = tag.delivery_tag;
    let (tx, rx) = oneshot::channel();

    queue
        .send(QueueCommand::GetMessage {
            tag: Some(tag.clone()),
            result: tx,
        })
        .await?;

    match rx.await? {
        Some(message) => {
            let frames = message_to_frames(&message, &tag);

            match send_message(&sink, frames).await {
                Err(e) => Ok(SendResult::ConsumerInvalid),
                _ => Ok(SendResult::MessageSent),
            }
        }
        None => Ok(SendResult::QueueEmpty),
    }
}

/// Send the message as frames to a consumer. Returns true if all the frames managed to
/// be sent to the channel. The caller of this function should take care of the result,
/// and in case of a failed sending, it should try to send to another consumer, or
/// if there is no consumer, it should store the message.
async fn send_message(consumer: &FrameSink, frames: Vec<AMQPFrame>) -> Result<bool> {
    let mut n: usize = 0;

    'frames: for f in &frames {
        debug!("Sending frame {:?}", f);

        // TODO here we can pass frame and get back from the SendError
        if let Err(e) = consumer.send_timeout(f.clone(), time::Duration::from_secs(1)).await {
            // TODO remove this channel from the consumers
            error!("Message send error {:?}", e);
            break 'frames;
        } else {
            n += 1
        }
    }

    Ok(n == frames.len())
}

fn message_to_frames(message: &Message, tag: &Tag) -> Vec<frame::AMQPFrame> {
    let mut frames = message::message_to_content_frames(&message);

    let basic_deliver = frame::basic_deliver(
        message.channel,
        &tag.consumer_tag,
        tag.delivery_tag,
        false,
        &message.exchange,
        &message.routing_key,
    );
    frames.insert(0, basic_deliver);

    frames
}

async fn get_message_from_queue_and_send(
    mut consumer: &mut Consumer,
    queue: &mpsc::Sender<QueueCommand>,
) -> Result<()> {
    let tag = Tag {
        consumer_tag: consumer.consumer_tag.clone(),
        delivery_tag: consumer.delivery_tag_counter,
    };

    let (tx, rx) = oneshot::channel();

    if let Err(e) = queue
        .send(QueueCommand::GetMessage {
            tag: Some(tag.clone()),
            result: tx,
        })
        .await
    {
        error!("Error {:?}", e);

        return Ok(());
    }

    match rx.await {
        Ok(Some(message)) => {
            let frames = message_to_frames(&message, &tag);

            if let Err(e) = send_message(&consumer.sink, frames).await {
                error!("Error {:?}", e);
            }
        }
        Ok(None) => (), // queue is empty
        Err(e) => {
            error!("Error {:?}", e);
        }
    }

    consumer.delivery_tag_counter += 1;

    Ok(())
}
