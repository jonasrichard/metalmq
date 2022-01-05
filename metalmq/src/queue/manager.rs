use crate::client::{channel_error, ChannelError};
use crate::queue::handler::{self, QueueCommand, QueueCommandSink};
use crate::queue::Queue;
use crate::{chk, logerr, send, Result};
use log::error;
use metalmq_codec::codec::Frame;
use metalmq_codec::frame;
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

// QueueManager thread
//   handles:
//     - declare queue (so create queue)
//     - get queue tokio channel
//     - consume queue
//     - delete queue
//
//  Queue handler:
//    Representation as an AMQP queue
//    handles:
//      - enqueue a message (basic publish)
//      - get a message for delivery (message or none)
//      - ack a message asked for delivery
//
//  Consumer handler
//    The consumer loop, seeing if there is any new messages and sends them
//    handles:
//      - new consumer
//      - cancel consumer
//      - new message was published
//      - reject message
//      - recover (redeliver unacked messages on that channel) - check the spec
//        https://www.rabbitmq.com/amqp-0-9-1-reference.html#class.basic

#[derive(Debug)]
struct QueueState {
    pub queue: Queue,
    pub command_sink: QueueCommandSink,
}

#[derive(Debug)]
pub struct QueueDeclareCommand {
    pub queue: Queue,
    pub conn_id: String,
    pub channel: u16,
}

#[derive(Debug)]
pub struct QueueConsumeCommand {
    pub conn_id: String,
    pub channel: u16,
    pub queue_name: String,
    pub consumer_tag: String,
    pub no_ack: bool,
    pub exclusive: bool,
    pub outgoing: mpsc::Sender<Frame>,
}

#[derive(Debug)]
pub struct QueueCancelConsume {
    pub channel: u16,
    pub queue_name: String,
    pub consumer_tag: String,
}

#[derive(Debug)]
pub struct GetQueueSinkQuery {
    pub channel: u16,
    pub queue_name: String,
}

#[derive(Debug)]
pub enum QueueManagerCommand {
    Declare(QueueDeclareCommand, oneshot::Sender<Result<()>>),
    Consume(QueueConsumeCommand, oneshot::Sender<Result<QueueCommandSink>>),
    CancelConsume(QueueCancelConsume, oneshot::Sender<Result<()>>),
    GetQueueSink(GetQueueSinkQuery, oneshot::Sender<Result<QueueCommandSink>>),
    GetQueues(oneshot::Sender<Vec<Queue>>),
}

pub type QueueManagerSink = mpsc::Sender<QueueManagerCommand>;

pub fn start() -> QueueManagerSink {
    let (sink, stream) = mpsc::channel(1);

    tokio::spawn(async move {
        let mut manager = QueueManagerState {
            command_stream: stream,
            queues: HashMap::new(),
        };

        if let Err(e) = manager.command_loop().await {
            error!("Queue manager exited {:?}", e);
        }
    });

    sink
}

pub async fn declare_queue(mgr: &QueueManagerSink, cmd: QueueDeclareCommand) -> Result<()> {
    let (tx, rx) = oneshot::channel();

    send!(mgr, QueueManagerCommand::Declare(cmd, tx))?;

    rx.await?
}

pub async fn consume(mgr: &QueueManagerSink, cmd: QueueConsumeCommand) -> Result<QueueCommandSink> {
    let (tx, rx) = oneshot::channel();

    send!(mgr, QueueManagerCommand::Consume(cmd, tx))?;

    rx.await?
}

pub async fn cancel_consume(mgr: &QueueManagerSink, cmd: QueueCancelConsume) -> Result<()> {
    let (tx, rx) = oneshot::channel();

    chk!(send!(mgr, QueueManagerCommand::CancelConsume(cmd, tx)))?;

    rx.await?
}

pub async fn get_command_sink(mgr: &QueueManagerSink, cmd: GetQueueSinkQuery) -> Result<QueueCommandSink> {
    let (tx, rx) = oneshot::channel();

    send!(mgr, QueueManagerCommand::GetQueueSink(cmd, tx))?;

    rx.await?
}

pub async fn get_queues(mgr: &QueueManagerSink) -> Vec<Queue> {
    let (tx, rx) = oneshot::channel();

    logerr!(mgr.send(QueueManagerCommand::GetQueues(tx)).await);

    match rx.await {
        Ok(queues) => queues,
        Err(_) => vec![],
    }
}

struct QueueManagerState {
    command_stream: mpsc::Receiver<QueueManagerCommand>,
    queues: HashMap<String, QueueState>,
}

impl QueueManagerState {
    async fn command_loop(&mut self) -> Result<()> {
        use QueueManagerCommand::*;

        while let Some(command) = self.command_stream.recv().await {
            match command {
                Declare(cmd, tx) => {
                    logerr!(tx.send(self.handle_declare(cmd).await));
                }
                Consume(cmd, tx) => {
                    logerr!(tx.send(self.handle_consume(cmd).await));
                }
                CancelConsume(cmd, tx) => {
                    match self.handle_cancel(cmd).await {
                        Ok(still_alive) => {
                            logerr!(tx.send(Ok(())));

                            if !still_alive {
                                // Queue is auto delete and the last consumer has cancelled.
                                break;
                            }
                        }
                        Err(e) => {
                            error!("Error {:?}", e);

                            logerr!(tx.send(Ok(())));
                        }
                    }
                }
                GetQueueSink(cmd, tx) => {
                    logerr!(tx.send(self.handle_get_command_sink(cmd)));
                }
                GetQueues(tx) => {
                    let qs = self.queues.iter().map(|kv| kv.1.queue.clone()).collect();

                    logerr!(tx.send(qs));
                }
            }
        }

        Ok(())
    }

    /// Declare queue with the given parameters. Declare means if the queue hasn't existed yet, it
    /// creates that.
    async fn handle_declare(&mut self, command: QueueDeclareCommand) -> Result<()> {
        // TODO implement different queue properties (exclusive, auto-delete, durable, properties)
        match self.queues.get(&command.queue.name) {
            // FIXME we need to check here if in case of passive declare the properties match or
            // we need to raise an error if queue is already declared
            Some(_) => Ok(()),
            None => {
                let (cmd_tx, mut cmd_rx) = mpsc::channel(1);
                let queue_name = command.queue.name.clone();
                let queue_state = QueueState {
                    queue: command.queue.clone(),
                    command_sink: cmd_tx,
                };

                tokio::spawn(async move {
                    handler::start(command.queue, command.conn_id, &mut cmd_rx).await;
                });

                self.queues.insert(queue_name, queue_state);

                Ok(())
            }
        }
    }

    async fn handle_consume(&self, command: QueueConsumeCommand) -> Result<QueueCommandSink> {
        match self.queues.get(&command.queue_name) {
            Some(queue) => {
                let (tx, rx) = oneshot::channel();

                send!(
                    queue.command_sink,
                    QueueCommand::StartConsuming {
                        conn_id: command.conn_id,
                        channel: command.channel,
                        consumer_tag: command.consumer_tag,
                        no_ack: command.no_ack,
                        exclusive: command.exclusive,
                        sink: command.outgoing,
                        result: tx,
                    }
                )?;

                if let Err(e) = rx.await? {
                    error!("Error on queue {} {:?}", command.queue_name, e);

                    return Err(e);
                }

                Ok(queue.command_sink.clone())
            }
            None => channel_error(
                command.channel,
                frame::BASIC_CONSUME,
                ChannelError::NotFound,
                "Not found",
            ),
        }
    }

    async fn handle_cancel(&self, command: QueueCancelConsume) -> Result<bool> {
        match self.queues.get(&command.queue_name) {
            Some(queue) => {
                let (tx, rx) = oneshot::channel();

                send!(
                    queue.command_sink,
                    QueueCommand::CancelConsuming {
                        consumer_tag: command.consumer_tag,
                        result: tx,
                    }
                )?;

                Ok(rx.await?)
            }
            None => Ok(true),
        }
    }

    fn handle_get_command_sink(&self, command: GetQueueSinkQuery) -> Result<QueueCommandSink> {
        match self.queues.get(&command.queue_name) {
            Some(queue) => Ok(queue.command_sink.clone()),
            None => channel_error(
                command.channel,
                frame::QUEUE_DECLARE,
                ChannelError::NotFound,
                "Not found",
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::tests;

    #[tokio::test]
    async fn declare_queue_works() {
        let queue_sink = start();
        let queue = Queue {
            name: "new-queue".to_string(),
            ..Default::default()
        };

        let cmd = QueueDeclareCommand {
            queue,
            conn_id: "conn_id".to_string(),
            channel: 4u16,
        };
        declare_queue(&queue_sink, cmd).await.unwrap();

        let cmd = GetQueueSinkQuery {
            channel: 4u16,
            queue_name: "new-queue".to_string(),
        };
        let qsink = get_command_sink(&queue_sink, cmd).await.unwrap();

        // TODO create func to generate message
        let message = tests::empty_message();

        let (tx, mut rx) = mpsc::channel(16);
        let cmd = QueueConsumeCommand {
            conn_id: "conn_id".to_string(),
            channel: 4u16,
            queue_name: "new-queue".to_string(),
            consumer_tag: "ctag".to_string(),
            no_ack: false,
            exclusive: false,
            outgoing: tx,
        };
        consume(&queue_sink, cmd).await.unwrap();

        qsink
            .send(QueueCommand::PublishMessage(Box::new(message)))
            .await
            .unwrap();

        let frames = rx.recv().await.unwrap();

        if let Frame::Frames(fs) = frames {
            // TODO make an assert function for checking the 3 frames
            assert_eq!(fs.len(), 3);
            // TODO it would be nice to have some function we can match the enum inside the method
            // frame like `let arg = get_arg_from_frame<T>(fs[0])` it should panic if it is not a
            // method frame and later we can assert or `assert!(matches!(arg, BasicCancelArgs {}))`
        }
    }
}
