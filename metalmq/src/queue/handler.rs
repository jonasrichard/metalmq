use crate::client::{channel_error, ChannelError};
use crate::message::{self, Message};
use crate::queue::Queue;
use crate::{logerr, Result};
use log::{error, trace};
use metalmq_codec::codec::Frame;
use metalmq_codec::frame::BASIC_CONSUME;
use std::collections::{HashSet, VecDeque};
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
        channel: u16,
        consumer_tag: String,
        no_ack: bool,
        exclusive: bool,
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
    ConsumerInvalid(String, Message),
}

pub(crate) type FrameSink = mpsc::Sender<Frame>;

/// Information about the queue instance
struct QueueState {
    queue: Queue,
    declaring_connection: String,
    messages: VecDeque<Message>,
    outbox: Outbox,
    consumers: Vec<Consumer>,
    next_consumer: usize,
    bound_exchanges: HashSet<String>,
    // TODO message metrics, current, current outgoing, etc...
}

struct Consumer {
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
        consumers: vec![],
        next_consumer: 0,
        bound_exchanges: HashSet::new(),
    }
    .queue_loop(commands)
    .await;
}

impl QueueState {
    pub(crate) async fn queue_loop(&mut self, mut commands: &mut mpsc::Receiver<QueueCommand>) {
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
            if !self.messages.is_empty() && !self.consumers.is_empty() {
                trace!("There are queued messages, sending out one...");

                let message = self.messages.pop_front().unwrap();

                logerr!(self.send_out_message(message).await);

                match poll_command_chan(&mut commands) {
                    Poll::Pending => (), // no commands, so we can keep sending out messages
                    Poll::Ready(Some(command)) => {
                        if let Ok(false) = self.handle_command(command).await {
                            break;
                        }
                    }
                    Poll::Ready(None) => {
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
        let start = Instant::now();

        trace!("Command {:?}", command);

        let r = match command {
            QueueCommand::PublishMessage(message) => {
                logerr!(self.send_out_message(message).await);

                Ok(true)
            }
            QueueCommand::AckMessage {
                consumer_tag,
                delivery_tag,
            } => {
                self.outbox.on_ack_arrive(consumer_tag, delivery_tag);
                Ok(true)
            }
            QueueCommand::ExchangeBound { exchange_name } => {
                self.bound_exchanges.insert(exchange_name);
                Ok(true)
            }
            QueueCommand::ExchangeUnbound { exchange_name } => {
                self.bound_exchanges.remove(&exchange_name);
                Ok(true)
            }
            QueueCommand::StartConsuming {
                conn_id,
                channel,
                consumer_tag,
                no_ack,
                exclusive,
                sink,
                result,
            } => {
                if self.queue.exclusive && self.declaring_connection != conn_id {
                    logerr!(result.send(channel_error(
                        channel,
                        BASIC_CONSUME,
                        ChannelError::ResourceLocked,
                        "Cannot consume exclusive queue"
                    )));
                } else if exclusive && !self.consumers.is_empty() {
                    logerr!(result.send(channel_error(
                        channel,
                        BASIC_CONSUME,
                        ChannelError::AccessRefused,
                        "Queue is already consumed, cannot consume exclusively"
                    )));
                } else {
                    let consumer = Consumer {
                        consumer_tag,
                        no_ack,
                        exclusive,
                        sink,
                        delivery_tag_counter: 1u64,
                    };
                    self.consumers.push(consumer);

                    logerr!(result.send(Ok(())));
                }

                Ok(true)
            }
            QueueCommand::CancelConsuming { consumer_tag, result } => {
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
            QueueCommand::MessageRejected => Ok(true),
            QueueCommand::Recover => Ok(true),
        };

        trace!("End {:?}", Instant::elapsed(&start));

        r
    }

    async fn send_out_message(&mut self, message: Message) -> Result<()> {
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

                let res = message::send_message(&message, &tag, &consumer.sink).await;
                match res {
                    Ok(()) => {
                        self.outbox.on_sent_out(OutgoingMessage {
                            message,
                            tag,
                            sent_at: Instant::now(),
                        });

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
}

fn poll_command_chan(commands: &mut mpsc::Receiver<QueueCommand>) -> Poll<Option<QueueCommand>> {
    use futures::task::noop_waker_ref;
    use std::task::Context;

    let mut cx = Context::from_waker(noop_waker_ref());
    commands.poll_recv(&mut cx)
}

struct OutgoingMessage {
    // TODO we don't need to store the whole message but rather the id
    message: Message,
    //message_id: String,
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use metalmq_codec::codec::Frame;

    fn default_queue_state() -> QueueState {
        let q = Queue {
            name: "test-queue".to_string(),
            ..Default::default()
        };

        QueueState {
            queue: q,
            declaring_connection: "conn-id".to_string(),
            messages: VecDeque::new(),
            outbox: Outbox {
                outgoing_messages: vec![],
            },
            bound_exchanges: HashSet::new(),
            consumers: vec![],
            next_consumer: 0,
        }
    }

    async fn recv_timeout(rx: &mut mpsc::Receiver<Frame>) -> Option<Frame> {
        let sleep = tokio::time::sleep(tokio::time::Duration::from_secs(1));
        tokio::pin!(sleep);

        tokio::select! {
            frame = rx.recv() => {
                return frame;
            }
            _ = &mut sleep => {
                return None;
            }
        }
    }

    #[tokio::test]
    async fn publish_to_queue_without_consumers() {
        let mut qs = default_queue_state();

        let cmd = QueueCommand::PublishMessage(Message {
            source_connection: "conn-id".to_string(),
            channel: 1,
            content: b"Hello".to_vec(),
            exchange: "test-exchange".to_string(),
            immediate: false,
            mandatory: false,
            routing_key: "".to_string(),
        });

        let result = qs.handle_command(cmd).await;
        assert!(result.is_ok());
        assert_eq!(qs.messages.len(), 1);

        let msg = qs.messages.get(0).unwrap();
        assert_eq!(msg.source_connection, "conn-id");
        assert_eq!(msg.channel, 1);
        assert_eq!(msg.content, b"Hello".to_vec());
    }

    #[tokio::test]
    async fn publish_to_queue_with_one_consumer() {
        use metalmq_codec::codec::Frame;
        use metalmq_codec::frame::{self, AMQPFrame};

        let mut qs = default_queue_state();
        let (msg_tx, mut msg_rx) = mpsc::channel(1);
        let (tx, rx) = oneshot::channel();

        let cmd = QueueCommand::StartConsuming {
            conn_id: "consumer-conn".to_string(),
            consumer_tag: "myctag".to_string(),
            channel: 1,
            no_ack: false,
            exclusive: false,
            sink: msg_tx,
            result: tx,
        };

        let result = qs.handle_command(cmd).await;
        assert!(result.is_ok());
        assert_eq!(qs.consumers.len(), 1);

        let cmd_result = rx.await;
        assert!(cmd_result.is_ok());

        let cmd = QueueCommand::PublishMessage(Message {
            source_connection: "conn-id".to_string(),
            channel: 1,
            content: b"Hello".to_vec(),
            exchange: "test-exchange".to_string(),
            immediate: false,
            mandatory: false,
            routing_key: "".to_string(),
        });

        let result = qs.handle_command(cmd).await;
        assert!(result.is_ok());

        let frame = recv_timeout(&mut msg_rx).await.unwrap();
        if let Frame::Frames(fs) = frame {
            if let AMQPFrame::Method(ch, cm, frame::MethodFrameArgs::BasicDeliver(args)) = fs.get(0).unwrap() {
                assert_eq!(ch, &1);
                assert_eq!(cm, &frame::BASIC_DELIVER);
                assert_eq!(args.consumer_tag, "myctag");
                assert_eq!(args.delivery_tag, 1);
            }
            // TODO check the other frames
        } else {
            println!("{:?}", frame);
            assert!(false, "Basic.Delivery frame is expected");
        }
    }
}
