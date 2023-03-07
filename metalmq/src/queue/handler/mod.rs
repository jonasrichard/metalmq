#[cfg(test)]
mod tests;

use crate::{
    client::{channel_error, ChannelError},
    exchange::{
        handler::QueueInfo,
        manager::{self as em, ExchangeManagerSink, QueueDeletedEvent},
    },
    logerr,
    message::{self, Message},
    queue::Queue,
    Result,
};
use log::{error, info, trace, warn};
use metalmq_codec::{codec::Frame, frame};
use std::{
    collections::{HashSet, VecDeque},
    sync::Arc,
    task::Poll,
    time::Instant,
};
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

#[derive(Debug)]
pub struct AckCmd {
    pub channel: u16,
    pub consumer_tag: String,
    pub delivery_tag: u64,
    pub multiple: bool,
    pub result: oneshot::Sender<Result<()>>,
}

#[derive(Debug)]
pub struct PassiveConsumeCmd {
    pub conn_id: String,
    pub channel: u16,
    pub sink: FrameSink,
    pub frame_size: usize,
    pub result: oneshot::Sender<Result<()>>,
}

#[derive(Debug)]
pub struct GetCmd {
    pub conn_id: String,
    pub channel: u16,
    pub no_ack: bool,
    pub result: oneshot::Sender<Result<()>>,
}

#[derive(Debug)]
pub struct PassiveCancelConsumeCmd {
    pub conn_id: String,
    pub channel: u16,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum QueueCommand {
    PublishMessage(Arc<Message>),
    /// Client acks a message.
    AckMessage(AckCmd),
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
    /// Even for a single Basic.Get the consumer is registered and treated as a normal active
    /// consumer.
    PassiveConsume(PassiveConsumeCmd),
    /// Client performs a basic get with or without acking.
    Get(GetCmd),
    PassiveCancelConsume(PassiveCancelConsumeCmd),
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
    /// Delete the exclusive queue, only queue manager can send this.
    DeleteExclusive {
        exchange_manager: ExchangeManagerSink,
        result: oneshot::Sender<Result<()>>,
    },
    MessageRejected,
    Recover,
    GetInfo {
        result: oneshot::Sender<QueueInfo>,
    },
}

#[derive(Debug)]
enum SendResult {
    MessageSent,
    //QueueEmpty,
    NoConsumer,
    ConsumerInvalid(String, DeliveredMessage),
}

pub type FrameSink = mpsc::Sender<Frame>;

#[derive(Clone, Debug)]
struct DeliveredMessage {
    message: Arc<Message>,
    /// How many times it tried to be delivered.
    delivery_count: u8,
}

/// Information about the queue instance
#[derive(Debug)]
struct QueueState {
    queue: Queue,
    declaring_connection: String,
    /// Messages represents the current message queue. It stores Arc references because we need to
    /// keep multiple copies of the same message (send that out, waiting for the ack, resending if
    /// there is no ack, etc).
    messages: VecDeque<DeliveredMessage>,
    outbox: Outbox,
    candidate_consumers: Vec<Consumer>,
    consumers: Vec<Consumer>,
    passive_consumers: Vec<Consumer>,
    next_consumer: usize,
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
#[derive(Debug)]
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
        passive_consumers: vec![],
        next_consumer: 0,
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
                let delivered_message = DeliveredMessage {
                    message,
                    delivery_count: 0,
                };

                logerr!(self.send_out_message(delivered_message).await);

                Ok(true)
            }
            QueueCommand::AckMessage(ack_cmd) => {
                self.handle_ack(ack_cmd).await.unwrap();

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
            QueueCommand::PassiveConsume(cmd) => {
                self.handle_passive_consume(cmd).await.unwrap();

                Ok(true)
            }
            QueueCommand::Get(cmd) => {
                self.handle_get(cmd).await.unwrap();

                Ok(true)
            }
            QueueCommand::PassiveCancelConsume(cmd) => {
                self.handle_passive_cancel_consume(cmd).await.unwrap();

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
                            .send(Frame::Frame(
                                frame::BasicCancelArgs::new(&consumer.consumer_tag).frame(consumer.channel)
                            ))
                            .await
                    );
                }

                // Cancel all candidate consumers by sending a basic cancel.
                for consumer in &self.candidate_consumers {
                    logerr!(
                        consumer
                            .sink
                            .send(Frame::Frame(
                                frame::BasicCancelArgs::new(&consumer.consumer_tag).frame(channel)
                            ))
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
            QueueCommand::DeleteExclusive {
                exchange_manager,
                result,
            } => {
                em::queue_deleted(
                    &exchange_manager,
                    em::QueueDeletedEvent {
                        queue_name: self.queue.name.clone(),
                    },
                )
                .await
                .unwrap();

                result.send(Ok(())).unwrap();

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

    // TODO Try to introduce functional programming style here, namely the sent messages should be
    // returned as a return value, and function should not have side effects - at least limit side
    // effects to small functions.

    async fn handle_passive_consume(&mut self, cmd: PassiveConsumeCmd) -> Result<()> {
        if self.queue.exclusive && cmd.conn_id != self.declaring_connection {
            cmd.result
                .send(channel_error(
                    cmd.channel,
                    frame::BASIC_GET,
                    ChannelError::ResourceLocked,
                    &format!("Queue {} is an exclusive queue of another connection", self.queue.name),
                ))
                .unwrap();

            return Ok(());
        }

        if self.consumers.iter().any(|c| c.exclusive) {
            cmd.result
                .send(channel_error(
                    cmd.channel,
                    frame::BASIC_GET,
                    ChannelError::AccessRefused,
                    &format!(
                        "Queue {} is already exclusively consumed by another connection",
                        self.queue.name
                    ),
                ))
                .unwrap();

            return Ok(());
        }

        // Passive consumers don't have consumer tags and that is a pity because this is how the
        // queue identifies them. So for passive consumers the queue generates a consumer tag from
        // the connection id and a channel number (in case of a client from two channels wants to
        // Basic.Get the same queue).
        let consumer = Consumer {
            channel: cmd.channel,
            consumer_tag: format!("{}-{}", cmd.conn_id, cmd.channel),
            no_ack: false,
            exclusive: false,
            sink: cmd.sink,
            delivery_tag_counter: 1u64,
            frame_size: cmd.frame_size,
        };

        self.passive_consumers.push(consumer);

        cmd.result.send(Ok(())).unwrap();

        Ok(())
    }

    async fn handle_get(&mut self, cmd: GetCmd) -> Result<Option<u64>> {
        let consumer_tag = format!("{}-{}", cmd.conn_id, cmd.channel);
        let pos = self
            .passive_consumers
            .iter()
            .position(|c| c.consumer_tag == consumer_tag);

        if pos.is_none() {
            cmd.result.send(Ok(())).unwrap();

            return Ok(None);
        }

        let consumer = self.passive_consumers.get_mut(pos.unwrap()).unwrap();

        if let Some(message) = self.messages.pop_front() {
            let delivery_tag = consumer.delivery_tag_counter;

            if !cmd.no_ack {
                let tag = Tag {
                    consumer_tag: consumer.consumer_tag.clone(),
                    delivery_tag,
                };

                self.outbox.on_sent_out(OutgoingMessage {
                    message: message.clone(),
                    tag,
                    sent_at: Instant::now(),
                });
            }

            message::send_basic_get_ok(
                cmd.channel,
                delivery_tag,
                message.delivery_count > 0,
                message.message,
                self.messages.len() as u32,
                consumer.frame_size,
                &consumer.sink,
            )
            .await
            .unwrap();

            cmd.result.send(Ok(())).unwrap();

            consumer.delivery_tag_counter += 1;

            Ok(Some(delivery_tag))
        } else {
            consumer
                .sink
                .send(Frame::Frame(frame::basic_get_empty(cmd.channel)))
                .await
                .unwrap();

            cmd.result.send(Ok(())).unwrap();

            Ok(None)
        }
    }

    async fn handle_passive_cancel_consume(&mut self, cmd: PassiveCancelConsumeCmd) -> Result<()> {
        let consumer_tag = format!("{}-{}", cmd.conn_id, cmd.channel);
        let pos = self
            .passive_consumers
            .iter()
            .position(|c| c.consumer_tag == consumer_tag);

        if pos.is_none() {
            warn!("Invalid passive consumer during cancellation {}", consumer_tag);

            return Ok(());
        }

        self.passive_consumers.remove(pos.unwrap());
        self.enqueue_outbox_messages(&consumer_tag);

        Ok(())
    }

    async fn handle_ack(&mut self, cmd: AckCmd) -> Result<()> {
        // TODO optimize consumer lookup
        if self
            .consumers
            .iter()
            .any(|c| c.consumer_tag == cmd.consumer_tag && c.delivery_tag_counter > cmd.delivery_tag)
            && self
                .passive_consumers
                .iter()
                .any(|c| c.consumer_tag == cmd.consumer_tag && c.delivery_tag_counter > cmd.delivery_tag)
        {
            cmd.result
                .send(channel_error(
                    cmd.channel,
                    frame::BASIC_ACK,
                    ChannelError::PreconditionFailed,
                    &format!(
                        "Client acked a non-delivered message with delivery tag {}",
                        cmd.delivery_tag
                    ),
                ))
                .unwrap();

            return Ok(());
        }

        if self
            .outbox
            .on_ack_arrive(cmd.consumer_tag, cmd.delivery_tag, cmd.multiple)
        {
            cmd.result.send(Ok(())).unwrap();
        } else {
            cmd.result
                .send(channel_error(
                    cmd.channel,
                    frame::BASIC_ACK,
                    ChannelError::PreconditionFailed,
                    &format!("Message is already acked with delivery tag {}", cmd.delivery_tag),
                ))
                .unwrap();
        }

        Ok(())
    }

    async fn send_out_message(&mut self, message: DeliveredMessage) -> Result<()> {
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
                    message.message.clone(),
                    &tag,
                    message.delivery_count > 0,
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

        for mut msg in msgs {
            msg.delivery_count += 1;

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

#[derive(Debug)]
struct OutgoingMessage {
    message: DeliveredMessage,
    tag: Tag,
    sent_at: Instant,
}

#[derive(Debug)]
struct Outbox {
    outgoing_messages: Vec<OutgoingMessage>,
}

impl Outbox {
    /// Acking messages by remove them based on the parameters. Return false if a message is double
    /// acked.
    fn on_ack_arrive(&mut self, consumer_tag: String, delivery_tag: u64, multiple: bool) -> bool {
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

    fn on_sent_out(&mut self, outgoing_message: OutgoingMessage) {
        self.outgoing_messages.push(outgoing_message);
    }

    fn remove_messages_by_ctag(&mut self, ctag: &str) -> Vec<DeliveredMessage> {
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
