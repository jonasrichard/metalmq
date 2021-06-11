use crate::logerr;
use crate::queue::handler::{QueueCommand, Tag};
use log::{error, info, trace};
use metalmq_codec::frame::AMQPFrame;
use std::cmp::Ordering;
use tokio::sync::{mpsc, oneshot};

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

#[derive(Debug)]
pub(crate) enum SendResult {
    MessageSent,
    QueueEmpty,
    ConsumerInvalid,
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

                trace!("Sending back result");
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
                    let (tx, rx) = oneshot::channel();

                    let r = queue
                        .send(QueueCommand::DeliverMessage {
                            tag: Tag {
                                consumer_tag: consumer.consumer_tag.clone(),
                                delivery_tag: consumer.delivery_tag_counter,
                            },
                            outgoing: consumer.sink.clone(),
                            result: tx,
                        })
                        .await;

                    logerr!(r);

                    trace!("Waiting");
                    match rx.await {
                        Ok(SendResult::MessageSent) => {
                            consumer.delivery_tag_counter += 1;
                        }
                        Ok(SendResult::QueueEmpty) => {
                            break 'consumer;
                        }
                        Ok(SendResult::ConsumerInvalid) => {
                            valid_consumer = false;

                            break 'consumer;
                        }
                        Err(e) => {
                            error!("Error {:?}", e);

                            break 'consumer;
                        }
                    }
                }

                if valid_consumer {
                    consumers.push(consumer);
                }
            }
            ConsumerCommand::CancelConsuming { consumer_tag, result } => {
                info!("Cancel consuming tag {}", consumer_tag);

                consumers.retain(|c| c.consumer_tag.cmp(&consumer_tag) == Ordering::Equal);

                logerr!(result.send(()));
            }
            ConsumerCommand::MessagePublished => {
                let mut invalid_ctags = vec![];

                for consumer in consumers.iter_mut() {
                    let (tx, rx) = oneshot::channel();

                    let r = queue
                        .send(QueueCommand::DeliverMessage {
                            tag: Tag {
                                consumer_tag: consumer.consumer_tag.clone(),
                                delivery_tag: consumer.delivery_tag_counter,
                            },
                            outgoing: consumer.sink.clone(),
                            result: tx,
                        })
                        .await;

                    logerr!(r);

                    trace!("Waiting");
                    match rx.await {
                        Ok(SendResult::MessageSent) => {
                            consumer.delivery_tag_counter += 1;
                        }
                        Ok(SendResult::QueueEmpty) => {
                            break;
                        }
                        Ok(SendResult::ConsumerInvalid) => {
                            invalid_ctags.push(consumer.consumer_tag.clone());
                        }
                        Err(e) => {
                            error!("Error {:?}", e);

                            invalid_ctags.push(consumer.consumer_tag.clone());
                        }
                    }
                }

                consumers.retain(|c| !invalid_ctags.contains(&c.consumer_tag));
            }
            _ => (),
        }
    }
}
