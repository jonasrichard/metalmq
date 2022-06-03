use crate::client::{channel_error, ChannelError};
use crate::exchange::manager::ExchangeManagerSink;
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
pub struct QueueDeleteCommand {
    pub channel: u16,
    pub queue_name: String,
    pub if_unused: bool,
    pub if_empty: bool,
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
    Delete(QueueDeleteCommand, oneshot::Sender<Result<u32>>),
    GetQueueSink(GetQueueSinkQuery, oneshot::Sender<Result<QueueCommandSink>>),
    GetQueues(oneshot::Sender<Vec<Queue>>),
}

pub type QueueManagerSink = mpsc::Sender<QueueManagerCommand>;

pub fn start(exchange_manager: ExchangeManagerSink) -> QueueManagerSink {
    let (sink, stream) = mpsc::channel(1);

    tokio::spawn(async move {
        let mut manager = QueueManagerState {
            command_stream: stream,
            queues: HashMap::new(),
            exchange_manager,
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

pub async fn delete_queue(mgr: &QueueManagerSink, cmd: QueueDeleteCommand) -> Result<u32> {
    let (tx, rx) = oneshot::channel();

    send!(mgr, QueueManagerCommand::Delete(cmd, tx))?;

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
    exchange_manager: ExchangeManagerSink,
}

impl QueueManagerState {
    async fn command_loop(&mut self) -> Result<()> {
        use QueueManagerCommand::*;

        while let Some(command) = self.command_stream.recv().await {
            match command {
                Declare(cmd, tx) => {
                    logerr!(tx.send(self.handle_declare(cmd).await));
                }
                Delete(cmd, tx) => {
                    logerr!(tx.send(self.handle_delete(cmd).await));
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

    async fn handle_delete(&mut self, command: QueueDeleteCommand) -> Result<u32> {
        match self.queues.get(&command.queue_name) {
            Some(queue) => {
                let (tx, rx) = oneshot::channel();

                send!(
                    queue.command_sink,
                    QueueCommand::DeleteQueue {
                        channel: command.channel,
                        if_unused: command.if_unused,
                        if_empty: command.if_empty,
                        exchange_manager: self.exchange_manager.clone(),
                        result: tx,
                    }
                )?;

                rx.await?
            }
            None => channel_error(
                command.channel,
                frame::QUEUE_DELETE,
                ChannelError::NotFound,
                "Not found",
            ),
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

    fn new_queue_manager() -> QueueManagerState {
        let (em_tx, _em_rx) = mpsc::channel(1);
        let (_cmd_tx, cmd_rx) = mpsc::channel(1);

        QueueManagerState {
            command_stream: cmd_rx,
            queues: HashMap::new(),
            exchange_manager: em_tx,
        }
    }

    fn queue_declare_command(channel: u16, queue_name: &str) -> QueueDeclareCommand {
        QueueDeclareCommand {
            queue: Queue {
                name: queue_name.to_string(),
                durable: false,
                exclusive: false,
                auto_delete: false,
            },
            conn_id: "conn_id".to_string(),
            channel,
        }
    }

    #[tokio::test]
    async fn queue_declare_manager_test() {
        let mut qm = new_queue_manager();

        qm.handle_declare(queue_declare_command(7u16, "test-queue"))
            .await
            .unwrap();

        assert_eq!(qm.queues.len(), 1);

        let queue_info = qm.queues.get("test-queue").unwrap();
        assert_eq!(queue_info.queue.name, "test-queue".to_string());
        assert_eq!(queue_info.queue.exclusive, false);
    }
}
