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

                let mut consumer = Consumer {
                    consumer_tag,
                    no_ack,
                    sink,
                    delivery_tag_counter: 1u64,
                };

                let mut valid_consumer = true;

                'consumer: loop {
                    // TODO if the message is not sent out, we need to put it back to the
                    // queue. Since we need to do that in every case, maybe we need to
                    // implement this in a function.
                    match get_message_from_queue_and_send(&mut consumer, &queue).await {
                        Ok(SendResult::QueueEmpty) => {
                            break 'consumer;
                        }
                        Ok(SendResult::ConsumerInvalid) => {
                            valid_consumer = false;

                            break 'consumer;
                        }
                        Err(e) => {
                            valid_consumer = false;

                            error!("Error {:?}", e);

                            break 'consumer;
                        }
                        _ => (),
                    }
                }

                if valid_consumer {
                    consumers.push(consumer);
                }
            }
            ConsumerCommand::CancelConsuming { consumer_tag, result } => {
                info!("Cancel consuming tag {}", consumer_tag);

                consumers.retain(|c| c.consumer_tag.cmp(&consumer_tag) == Ordering::Equal);

                result.send(());
            }
            ConsumerCommand::MessagePublished => {
                let mut invalid_ctags = vec![];

                for consumer in consumers.iter_mut() {
                    let tag = Tag {
                        consumer_tag: consumer.consumer_tag.clone(),
                        delivery_tag: consumer.delivery_tag_counter,
                    };

                    // TODO here we can drop the message if consumer is invalid of in case of
                    // other error
                    match get_message(&queue, tag.clone()).await {
                        Err(_) => break,
                        Ok(None) => break,
                        Ok(Some(message)) => {
                            let frames = message_to_frames(&message, &tag);

                            match send_message(&consumer.sink, frames).await {
                                Ok(SendResult::ConsumerInvalid) => invalid_ctags.push(consumer.consumer_tag.clone()),
                                Ok(_) => (),
                                Err(_) => (),
                            }
                        }
                    }
                }

                consumers.retain(|c| !invalid_ctags.contains(&c.consumer_tag));
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

async fn get_message(queue: &mpsc::Sender<QueueCommand>, tag: Tag) -> Result<Option<Message>> {
    let (tx, rx) = oneshot::channel();

    queue
        .send(QueueCommand::GetMessage {
            tag: Some(tag),
            result: tx,
        })
        .await?;

    match rx.await {
        Err(e) => Err(Box::new(e)),
        Ok(result) => Ok(result),
    }
}

async fn send_message(consumer: &FrameSink, frames: Vec<AMQPFrame>) -> Result<SendResult> {
    for f in &frames {
        debug!("Sending frame {:?}", f);

        // TODO here we can pass frame and get back from the SendError
        if let Err(_) = consumer.send_timeout(f.clone(), time::Duration::from_secs(1)).await {
            return Ok(SendResult::ConsumerInvalid);
        }
    }

    Ok(SendResult::MessageSent)
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
) -> Result<SendResult> {
    let tag = Tag {
        consumer_tag: consumer.consumer_tag.clone(),
        delivery_tag: consumer.delivery_tag_counter,
    };

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
            let result = send_message(&consumer.sink, frames).await?;

            if let SendResult::MessageSent = result {
                consumer.delivery_tag_counter += 1;
            }

            Ok(result)
        }
        None => Ok(SendResult::QueueEmpty),
    }
}
