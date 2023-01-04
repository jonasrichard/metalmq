#[cfg(test)]
mod tests;

use crate::client::{channel_error, ChannelError};
use crate::exchange::handler::QueueInfo;
use crate::exchange::manager::{self as em, ExchangeManagerSink, QueueDeletedEvent};
use crate::message::{self, Message};
use crate::queue::Queue;
use crate::{logerr, Result};
use log::{error, info, trace};
use metalmq_codec::codec::Frame;
use metalmq_codec::frame;
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::task::Poll;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot};

pub type QueueCommandSink = mpsc::Sender<QueueCommand>;

/// Delivery tag of a message
#[derive(Clone, Debug)]
pub struct Tag {
    /// The identifier of the consumer
    pub consumer_tag: String,
    /// The delivery tag - usually a monotonic number
    pub delivery_tag: u64,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum QueueCommand {
    PublishMessage(Arc<Message>),
    /// Client acks a message.
    AckMessage {
        consumer_tag: String,
        delivery_tag: u64,
    },
    /// In case of re-declaration or passive declaration of the queue, the Declare.GetOk message
    /// should contain the number of messages and current consumers count.
    GetDeclareOk {
        result: oneshot::Sender<(u32, u32)>,
    },
    ExchangeBound {
        conn_id: String,
        channel: u16,
        exchange_name: String,
        result: oneshot::Sender<Result<()>>,
    },
    ExchangeUnbound {
        exchange_name: String,
        result: oneshot::Sender<Result<()>>,
    },
    StartConsuming {
        conn_id: String,
        channel: u16,
        consumer_tag: String,
        no_ack: bool,
        exclusive: bool,
        sink: FrameSink,
        frame_size: usize,
        result: oneshot::Sender<Result<()>>,
    },
    StartDelivering {
        consumer_tag: String,
    },
    CancelConsuming {
        consumer_tag: String,
        result: oneshot::Sender<bool>,
    },
    /// Client performs a basic get with or without acking.
    Get {
        conn_id: String,
        channel: u16,
        no_ack: bool,
        sink: FrameSink,
        frame_size: usize,
        /// The delivery tag sent out or `None` if there has been no message sent.
        result: oneshot::Sender<Result<Option<u64>>>,
    },
    Purge {
        conn_id: String,
        channel: u16,
        result: oneshot::Sender<Result<u32>>,
    },
    DeleteQueue {
        conn_id: String,
        channel: u16,
        if_unused: bool,
        if_empty: bool,
        exchange_manager: ExchangeManagerSink,
        result: oneshot::Sender<Result<u32>>,
    },
    MessageRejected,
    Recover,
    GetInfo {
        result: oneshot::Sender<QueueInfo>,
    },
}

#[derive(Debug)]
pub enum SendResult {
    MessageSent,
    //QueueEmpty,
    NoConsumer,
    ConsumerInvalid(String, Arc<Message>),
}

pub type FrameSink = mpsc::Sender<Frame>;

/// Information about the queue instance
struct QueueState {
    queue: Queue,
    declaring_connection: String,
    /// Messages represents the current message queue. It stores Arc references because we need to
    /// keep multiple copies of the same message (send that out, waiting for the ack, resending if
    /// there is no ack, etc).
    messages: VecDeque<Arc<Message>>,
    outbox: Outbox,
    candidate_consumers: Vec<Consumer>,
    consumers: Vec<Consumer>,
    next_consumer: usize,
    /// An increasing counter for Basic.Get requests globally for all consumers of the queue.
    global_delivery_tag: u64,
    // TODO we need to store the routing key and headers and so on
    bound_exchanges: HashSet<String>,
    // TODO message metrics, current, current outgoing, etc...
}

// TODO consumer should have a counter of the in-flight messages.
// In that case we don't have the mpsc buffer full in the client
// because we know what is the size of the message buffer. On the
// other hand, we need to implement a receive message buffer in
// client, so we won't have problem when the client callback
// takes too much time to complete. Even though, right now we
// have only mpsc receiver-based client message processing, in
// case of a message buffer, we can make independent the receive
// of messages on client side, and to call the callback.
struct Consumer {
    /// The channel the consumer uses
    channel: u16,
    /// Consumer tag, identifies the consumer
    consumer_tag: String,
    /// Consumer doesn't need ack, so we can delete sent-out messages promptly
    no_ack: bool,
    /// If consumer is exclusive consumer
    exclusive: bool,
    /// Consumer network socket abstraction
    sink: FrameSink,
    /// The next delivery tag it needs to send out
    delivery_tag_counter: u64,
    /// The maximum frame size the consumer can process
    frame_size: usize,
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

pub async fn start(queue: Queue, declaring_connection: String, commands: &mut mpsc::Receiver<QueueCommand>) {
    QueueState {
        queue,
        declaring_connection,
        messages: VecDeque::new(),
        outbox: Outbox {
            outgoing_messages: vec![],
        },
        candidate_consumers: vec![],
        consumers: vec![],
        next_consumer: 0,
        global_delivery_tag: 0u64,
        bound_exchanges: HashSet::new(),
    }
    .queue_loop(commands)
    .await;
}

pub async fn purge(conn_id: String, channel: u16, sink: &mpsc::Sender<QueueCommand>) -> Result<u32> {
    let (tx, rx) = oneshot::channel();

    sink.send(QueueCommand::Purge {
        conn_id,
        channel,
        result: tx,
    })
    .await?;

    rx.await?
}

impl QueueState {
    pub async fn queue_loop(&mut self, commands: &mut mpsc::Receiver<QueueCommand>) {
        // TODO we need to store the delivery tags by consumers
        // Also we need to mark a message that it is sent, so we need to wait
        // for the ack, until that we cannot send new messages out - or depending
        // the consuming yes?
        loop {
            // TODO here we need to check if there are messages what we can send out,
            // if yes but there is no consumer yet, we need to remember that we already
            // tried and we don't go into an infinite loop. So in this case we can
            // safely go to the other branch doing blocking wait.
            // If we get a message we can clear this 'state flag'.
            //
            // FIXME When client sends basic consume, we reply with basic consume ok,
            // but right before that we start to deliver messages. That is a race
            // condition what we need to send with a coordination.
            // Probably we need to have StartConsuming and ConsumerSubscribed messages
            // and we need to wait the client state machine to send us an ok message
            // back, that it sent out the consume-ok message.
            if !self.messages.is_empty() && !self.consumers.is_empty() {
                trace!("There are queued messages, sending out one...");

                let message = self.messages.pop_front().unwrap();

                logerr!(self.send_out_message(message).await);

                match poll_command_chan(commands) {
                    Poll::Pending => (), // no commands, so we can keep sending out messages
                    Poll::Ready(Some(command)) => {
                        if let Ok(false) = self.handle_command(command).await {
                            break;
                        }
                    }
                    Poll::Ready(None) => {
                        // Command channel is closed, let us exit from the command queue loop.
                        break;
                    } // TODO break the loop and cleanup
                }
            } else {
                match commands.recv().await {
                    Some(command) => {
                        if let Ok(false) = self.handle_command(command).await {
                            break;
                        }
                    }
                    None => {
                        break;
                    }
                }
            }
        }
    }

    async fn handle_command(&mut self, command: QueueCommand) -> Result<bool> {
        //let start = Instant::now();

        match command {
            QueueCommand::PublishMessage(message) => {
                logerr!(self.send_out_message(message).await);

                Ok(true)
            }
            QueueCommand::AckMessage {
                consumer_tag,
                delivery_tag,
            } => {
                // TODO the message must not be acknowledged more than once (channel exception)
                self.outbox.on_ack_arrive(consumer_tag, delivery_tag);
                Ok(true)
            }
            QueueCommand::GetDeclareOk { result } => {
                let message_count = self.messages.len();
                let consumer_count = self.consumers.len();

                result.send((message_count as u32, consumer_count as u32)).unwrap();

                Ok(true)
            }
            QueueCommand::ExchangeBound {
                conn_id,
                channel,
                exchange_name,
                result,
            } => {
                if self.declaring_connection != conn_id {
                    result
                        .send(channel_error(
                            channel,
                            frame::QUEUE_BIND,
                            ChannelError::AccessRefused,
                            "Exclusive queue belongs to another connection",
                        ))
                        .unwrap();
                } else {
                    self.bound_exchanges.insert(exchange_name);
                    result.send(Ok(())).unwrap();
                }

                Ok(true)
            }
            QueueCommand::ExchangeUnbound { exchange_name, result } => {
                //if self.declaring_connection != conn_id {
                //    result.send(channel_error(
                //        channel,
                //        frame::QUEUE_BIND,
                //        ChannelError::AccessRefused,
                //        "Exclusive queue belongs to another connection",
                //    ));
                //} else {
                self.bound_exchanges.remove(&exchange_name);
                result.send(Ok(())).unwrap();
                //}

                Ok(true)
            }
            QueueCommand::StartDelivering { consumer_tag } => {
                if let Some(p) = self
                    .candidate_consumers
                    .iter()
                    .position(|c| c.consumer_tag.eq(&consumer_tag))
                {
                    let consumer = self.candidate_consumers.remove(p);

                    self.consumers.push(consumer);
                }
                Ok(true)
            }
            QueueCommand::StartConsuming {
                conn_id,
                channel,
                consumer_tag,
                no_ack,
                exclusive,
                sink,
                frame_size,
                result,
            } => {
                info!(
                    "Queue {} is consumed by {} ctag is {}",
                    self.queue.name, conn_id, consumer_tag
                );

                if self.queue.exclusive && self.declaring_connection != conn_id {
                    logerr!(result.send(channel_error(
                        channel,
                        frame::BASIC_CONSUME,
                        ChannelError::ResourceLocked,
                        "Cannot consume exclusive queue"
                    )));
                } else if exclusive && !self.consumers.is_empty() {
                    logerr!(result.send(channel_error(
                        channel,
                        frame::BASIC_CONSUME,
                        ChannelError::AccessRefused,
                        "Queue is already consumed, cannot consume exclusively"
                    )));
                } else {
                    let consumer = Consumer {
                        channel,
                        consumer_tag,
                        no_ack,
                        exclusive,
                        sink,
                        delivery_tag_counter: 1u64,
                        frame_size,
                    };
                    self.candidate_consumers.push(consumer);

                    logerr!(result.send(Ok(())));
                }

                Ok(true)
            }
            QueueCommand::CancelConsuming { consumer_tag, result } => {
                info!(
                    "Queue {} is stopped consuming by ctag {}",
                    self.queue.name, consumer_tag
                );

                self.enqueue_outbox_messages(&consumer_tag);

                self.consumers.retain(|c| !c.consumer_tag.eq(&consumer_tag));
                self.next_consumer = 0;

                if self.queue.auto_delete && self.consumers.is_empty() {
                    logerr!(result.send(false));

                    Ok(false)
                } else {
                    logerr!(result.send(true));

                    Ok(true)
                }
            }
            QueueCommand::Get {
                conn_id,
                channel,
                no_ack,
                sink,
                frame_size,
                result,
            } => {
                if self.queue.exclusive && conn_id != self.declaring_connection {
                    result
                        .send(channel_error(
                            channel,
                            frame::BASIC_GET,
                            ChannelError::ResourceLocked,
                            &format!("Queue {} is an exclusive queue of another connection", self.queue.name),
                        ))
                        .unwrap();

                    return Ok(true);
                }

                if self.consumers.iter().any(|c| c.exclusive) {
                    result
                        .send(channel_error(
                            channel,
                            frame::BASIC_GET,
                            ChannelError::AccessRefused,
                            &format!(
                                "Queue {} is already exclusively consumed by another connection",
                                self.queue.name
                            ),
                        ))
                        .unwrap();

                    return Ok(true);
                }

                if let Some(message) = self.messages.pop_front() {
                    if !no_ack {
                        let tag = Tag {
                            consumer_tag: "".to_string(),
                            delivery_tag: self.global_delivery_tag,
                        };

                        self.outbox.on_sent_out(OutgoingMessage {
                            message: message.clone(),
                            tag,
                            sent_at: Instant::now(),
                        });
                    }

                    message::send_basic_get_ok(
                        channel,
                        self.global_delivery_tag,
                        message,
                        self.messages.len() as u32,
                        frame_size,
                        &sink,
                    )
                    .await
                    .unwrap();

                    result.send(Ok(Some(self.global_delivery_tag))).unwrap();

                    self.global_delivery_tag += 1;
                } else {
                    sink.send(Frame::Frame(frame::basic_get_empty(channel))).await.unwrap();

                    result.send(Ok(None)).unwrap();
                }

                Ok(true)
            }
            QueueCommand::Purge {
                conn_id,
                channel,
                result,
            } => {
                // TODO you cannot purge an exclusive queue
                let message_count = self.messages.len();
                self.messages.clear();

                logerr!(result.send(Ok(message_count as u32)));

                Ok(true)
            }
            QueueCommand::DeleteQueue {
                conn_id,
                channel,
                if_unused,
                if_empty,
                exchange_manager,
                result,
            } => {
                info!("Queue {} is about to be deleted", self.queue.name);

                // If there are consumers or candidates or if there are exchanges bound to this
                // queue, and we cannot delete if it is used, send back and error.
                if if_unused {
                    if !self.consumers.is_empty() || !self.candidate_consumers.is_empty() {
                        logerr!(result.send(channel_error(
                            channel,
                            frame::QUEUE_DELETE,
                            ChannelError::PreconditionFailed,
                            "Queue is consumed"
                        )));

                        return Ok(true);
                    }

                    if !self.bound_exchanges.is_empty() {
                        logerr!(result.send(channel_error(
                            channel,
                            frame::QUEUE_DELETE,
                            ChannelError::PreconditionFailed,
                            "Exchanges are bound to this queue"
                        )));

                        return Ok(true);
                    }
                }

                if if_empty && !self.messages.is_empty() {
                    logerr!(result.send(channel_error(
                        channel,
                        frame::QUEUE_DELETE,
                        ChannelError::PreconditionFailed,
                        "Queue is not empty"
                    )));

                    return Ok(true);
                }

                // Notify all exchanges about the delete, so they can unbound themselves.
                for _ in &self.bound_exchanges {
                    let queue_deleted_evt = QueueDeletedEvent {
                        queue_name: self.queue.name.clone(),
                    };

                    logerr!(em::queue_deleted(&exchange_manager, queue_deleted_evt).await);
                }

                // Cancel all consumers by sending a basic cancel.
                for consumer in &self.consumers {
                    logerr!(
                        consumer
                            .sink
                            .send(Frame::Frame(frame::basic_cancel(
                                consumer.channel,
                                "Queue is deleted",
                                false
                            )))
                            .await
                    );
                }

                // Cancel all candidate consumers by sending a basic cancel.
                for consumer in &self.candidate_consumers {
                    logerr!(
                        consumer
                            .sink
                            .send(Frame::Frame(frame::basic_cancel(
                                consumer.channel,
                                "Queue is deleted",
                                false
                            )))
                            .await
                    );
                }

                // TODO Cancel all passive consumers, I think this is silent, I don't know what we need
                // to send back if there is a message waiting to be acked.

                // Pass the number of messages in the queue to the caller.
                logerr!(result.send(Ok(self.messages.len() as u32)));

                // Quit the queue event loop.
                Ok(false)
            }
            QueueCommand::MessageRejected => Ok(true),
            QueueCommand::Recover => Ok(true),
            QueueCommand::GetInfo { result } => {
                result
                    .send(QueueInfo {
                        queue_name: self.queue.name.clone(),
                        declaring_connection: self.declaring_connection.clone(),
                        exclusive: self.queue.exclusive,
                        durable: self.queue.durable,
                        auto_delete: self.queue.auto_delete,
                    })
                    .unwrap();

                Ok(true)
            }
        }
    }

    async fn send_out_message(&mut self, message: Arc<Message>) -> Result<()> {
        let res = match self.consumers.get(self.next_consumer) {
            None => {
                trace!("No consumers, pushing message back to the queue");

                self.messages.push_back(message);

                SendResult::NoConsumer
            }
            Some(consumer) => {
                let tag = Tag {
                    consumer_tag: consumer.consumer_tag.clone(),
                    delivery_tag: consumer.delivery_tag_counter,
                };

                let res = message::send_message(
                    consumer.channel,
                    message.clone(),
                    &tag,
                    consumer.frame_size,
                    &consumer.sink,
                )
                .await;

                match res {
                    Ok(()) => {
                        if !consumer.no_ack {
                            self.outbox.on_sent_out(OutgoingMessage {
                                message,
                                tag,
                                sent_at: Instant::now(),
                            });
                        }

                        if let Some(p) = self
                            .consumers
                            .iter()
                            .position(|c| c.consumer_tag.eq(&consumer.consumer_tag))
                        {
                            self.consumers[p].delivery_tag_counter += 1;
                        }

                        SendResult::MessageSent
                    }
                    Err(e) => {
                        error!("Consumer sink seems to be invalid {:?}", e);

                        SendResult::ConsumerInvalid(consumer.consumer_tag.clone(), message)
                    }
                }
            }
        };

        match res {
            SendResult::ConsumerInvalid(ctag, msg) => {
                self.messages.push_back(msg);
                self.consumers.retain(|c| !c.consumer_tag.eq(&ctag));
                self.next_consumer = 0;
            }
            SendResult::MessageSent => {
                self.next_consumer = (self.next_consumer + 1) % self.consumers.len();
            }
            _ => (),
        }

        Ok(())
    }

    /// Put back the outgoing messages which were sent out to a consumer but the consumer cancelled
    /// without acking those messages.
    fn enqueue_outbox_messages(&mut self, consumer_tag: &str) {
        let mut msgs = self.outbox.remove_messages_by_ctag(consumer_tag);
        msgs.reverse();

        for msg in msgs {
            self.messages.push_front(msg);
        }
    }
}

fn poll_command_chan(commands: &mut mpsc::Receiver<QueueCommand>) -> Poll<Option<QueueCommand>> {
    use futures::task::noop_waker_ref;
    use std::task::Context;

    let mut cx = Context::from_waker(noop_waker_ref());
    commands.poll_recv(&mut cx)
}

struct OutgoingMessage {
    message: Arc<Message>,
    tag: Tag,
    sent_at: Instant,
}

struct Outbox {
    outgoing_messages: Vec<OutgoingMessage>,
}

impl Outbox {
    fn on_ack_arrive(&mut self, consumer_tag: String, delivery_tag: u64) {
        self.outgoing_messages
            .retain(|om| om.tag.delivery_tag != delivery_tag || om.tag.consumer_tag != consumer_tag);
    }

    fn on_sent_out(&mut self, outgoing_message: OutgoingMessage) {
        self.outgoing_messages.push(outgoing_message);
    }

    fn remove_messages_by_ctag(&mut self, ctag: &str) -> Vec<Arc<Message>> {
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
