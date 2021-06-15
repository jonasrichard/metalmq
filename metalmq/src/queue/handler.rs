use crate::client::{channel_error, ChannelError};
use crate::message::{self, Message};
use crate::queue::Queue;
use crate::{logerr, Result};
use log::{error, trace};
use metalmq_codec::frame::{AMQPFrame, BASIC_CONSUME};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use std::task::Poll;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot};

pub(crate) type QueueCommandSink = mpsc::Sender<QueueCommand>;
//pub(crate) type FrameStream = mpsc::Receiver<frame::AMQPFrame>;

#[derive(Clone, Debug)]
pub(crate) struct Tag {
    pub consumer_tag: String,
    pub delivery_tag: u64,
}

#[derive(Debug)]
pub(crate) enum QueueCommand {
    PublishMessage(Message),
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
    StartConsuming {
        conn_id: String,
        consumer_tag: String,
        no_ack: bool,
        sink: FrameSink,
        result: oneshot::Sender<Result<()>>,
    },
    CancelConsuming {
        consumer_tag: String,
        result: oneshot::Sender<bool>,
    },
    MessageRejected,
    Recover,
}

#[derive(Debug)]
pub(crate) enum SendResult {
    MessageSent,
    QueueEmpty,
    NoConsumer,
    ConsumerInvalid,
}

pub(crate) type FrameSink = mpsc::Sender<AMQPFrame>;

/// Information about the queue instance
struct QueueState {
    queue: Queue,
    declaring_connection: String,
    messages: VecDeque<Message>,
    outbox: Outbox,
    consumers: HashMap<String, Consumer>,
    bound_exchanges: HashSet<String>,
    // TODO message metrics, current, current outgoing, etc...
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

pub(crate) async fn start(queue: Queue, declaring_connection: String, commands: &mut mpsc::Receiver<QueueCommand>) {
    QueueState {
        queue,
        declaring_connection,
        messages: VecDeque::new(),
        outbox: Outbox {
            outgoing_messages: vec![],
        },
        consumers: HashMap::new(),
        bound_exchanges: HashSet::new(),
    }
    .queue_loop(commands)
    .await;
}

impl QueueState {
    pub(crate) async fn other(&mut self, mut commands: &mut mpsc::Receiver<QueueCommand>) {
        loop {
            if self.messages.len() > 0 {
                let message = self.messages.pop_front().unwrap();
                self.send_out_message(message).await;

                match poll_command_chan(&mut commands) {
                    Poll::Pending => (),              // no commands, so we can keep sending out messages
                    Poll::Ready(Some(command)) => (), // TODO handle the command
                    Poll::Ready(None) => (),          // TODO break the loop and cleanup
                }
            } else {
                match commands.recv().await {
                    Some(command) => (),
                    None => {
                        break;
                    }
                }
            }
        }
    }

    pub(crate) async fn queue_loop(&mut self, commands: &mut mpsc::Receiver<QueueCommand>) {
        // TODO we need to store the delivery tags by consumers
        // Also we need to mark a message that it is sent, so we need to wait
        // for the ack, until that we cannot send new messages out - or depending
        // the consuming yes?

        while let Some(command) = commands.recv().await {
            trace!("Queue command {:?}", command);

            match command {
                QueueCommand::PublishMessage(message) => {
                    trace!("Queue message {:?}", message);

                    logerr!(self.send_out_message(message).await);
                }
                QueueCommand::AckMessage {
                    consumer_tag,
                    delivery_tag,
                } => {
                    self.outbox.on_ack_arrive(consumer_tag, delivery_tag);
                }
                QueueCommand::ExchangeBound { exchange_name } => {
                    self.bound_exchanges.insert(exchange_name);
                }
                QueueCommand::ExchangeUnbound { exchange_name } => {
                    self.bound_exchanges.remove(&exchange_name);
                }
                QueueCommand::StartConsuming {
                    conn_id,
                    consumer_tag,
                    no_ack,
                    sink,
                    result,
                } => {
                    if self.queue.exclusive && self.declaring_connection.cmp(&conn_id) != Ordering::Equal {
                        logerr!(result.send(channel_error(
                            0,
                            BASIC_CONSUME,
                            ChannelError::ResourceLocked,
                            "Cannot consume exclusive queue"
                        )));
                    } else {
                        let consumer = Consumer {
                            consumer_tag: consumer_tag.clone(),
                            no_ack,
                            sink,
                            delivery_tag_counter: 1u64,
                        };
                        self.consumers.insert(consumer_tag, consumer);
                        logerr!(result.send(Ok(())));

                        logerr!(self.send_out_all_messages().await);
                    }
                }
                QueueCommand::CancelConsuming { consumer_tag, result } => {
                    self.consumers.remove(&consumer_tag);

                    if self.queue.auto_delete && self.consumers.len() == 0 {
                        logerr!(result.send(false));

                        break;
                    } else {
                        logerr!(result.send(true));
                    }
                }
                QueueCommand::MessageRejected => {}
                QueueCommand::Recover => {}
            }
        }
    }

    fn choose_consumer(&self) -> Option<&Consumer> {
        match self.consumers.iter().next() {
            Some((_, v)) => Some(v),
            None => None,
        }
    }

    async fn send_out_all_messages(&mut self) -> Result<()> {
        while let Some(message) = self.messages.pop_front() {
            match self.send_out_message(message).await {
                Ok(SendResult::MessageSent) => (),
                Ok(SendResult::QueueEmpty) => {
                    break;
                }
                Ok(SendResult::NoConsumer) => {
                    break;
                }
                Ok(SendResult::ConsumerInvalid) => (), // TODO remove consumer and get back message
                Err(e) => {
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    async fn send_out_message(&mut self, message: Message) -> Result<SendResult> {
        let mut chosen_ctag = None;

        let res = match self.consumers.iter().next() {
            None => {
                self.messages.push_back(message);

                Ok(SendResult::NoConsumer)
            }
            Some((_, consumer)) => {
                let tag = Tag {
                    consumer_tag: consumer.consumer_tag.clone(),
                    delivery_tag: consumer.delivery_tag_counter,
                };

                chosen_ctag = Some(consumer.consumer_tag.clone());

                let res = message::send_message(&message, &tag, &consumer.sink).await;
                match res {
                    Ok(()) => {
                        self.outbox.on_sent_out(OutgoingMessage {
                            message,
                            tag,
                            sent_at: Instant::now(),
                        });

                        Ok(SendResult::MessageSent)
                    }
                    Err(_) => Ok(SendResult::ConsumerInvalid),
                }
            }
        };

        match res {
            Ok(SendResult::MessageSent) => {
                if let Some(c) = self.consumers.get_mut(&chosen_ctag.unwrap()) {
                    c.delivery_tag_counter += 1;
                }
            }
            Ok(SendResult::ConsumerInvalid) => {
                self.consumers.remove(&chosen_ctag.unwrap());
            }
            _ => (),
        }

        res
    }
}

fn poll_command_chan(commands: &mut mpsc::Receiver<QueueCommand>) -> Poll<Option<QueueCommand>> {
    use futures_util::task::noop_waker_ref;
    use std::task::Context;

    let mut cx = Context::from_waker(noop_waker_ref());
    commands.poll_recv(&mut cx)
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
