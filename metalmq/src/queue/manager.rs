use crate::client::{channel_error, ChannelError};
use crate::queue::handler::{self, QueueCommand, QueueCommandSink};
use crate::queue::Queue;
use crate::{chk, logerr, Result};
use log::{debug, error};
use metalmq_codec::frame::{self, AMQPFrame};
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};
use tokio::time;

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
    queue: Queue,
    command_sink: QueueCommandSink,
}

#[derive(Debug)]
pub(crate) enum QueueManagerCommand {
    Declare {
        queue: Queue,
        conn_id: String,
        result: oneshot::Sender<Result<()>>,
    },
    Consume {
        conn_id: String,
        queue_name: String,
        consumer_tag: String,
        no_ack: bool,
        outgoing: mpsc::Sender<AMQPFrame>,
        result: oneshot::Sender<Result<QueueCommandSink>>,
    },
    CancelConsume {
        queue_name: String,
        consumer_tag: String,
        result: oneshot::Sender<Result<()>>,
    },
    GetQueueSink {
        queue_name: String,
        result: oneshot::Sender<Result<QueueCommandSink>>,
    },
}

pub(crate) type QueueManagerSink = mpsc::Sender<QueueManagerCommand>;

pub(crate) fn start() -> QueueManagerSink {
    let (sink, stream) = mpsc::channel(1);

    tokio::spawn(async move {
        if let Err(e) = command_loop(stream).await {
            error!("Queue manager exited {:?}", e);
        }
    });

    sink
}

pub(crate) async fn declare_queue(mgr: &QueueManagerSink, queue: Queue, conn_id: &str) -> Result<()> {
    let (tx, rx) = oneshot::channel();

    mgr.send(QueueManagerCommand::Declare {
        queue,
        conn_id: conn_id.to_string(),
        result: tx,
    })
    .await?;

    rx.await?
}

pub(crate) async fn consume(
    mgr: &QueueManagerSink,
    conn_id: &str,
    queue_name: &str,
    consumer_tag: &str,
    no_ack: bool,
    outgoing: mpsc::Sender<AMQPFrame>,
) -> Result<QueueCommandSink> {
    let (tx, rx) = oneshot::channel();

    mgr.send(QueueManagerCommand::Consume {
        conn_id: conn_id.to_string(),
        queue_name: queue_name.to_string(),
        consumer_tag: consumer_tag.to_string(),
        no_ack,
        outgoing,
        result: tx,
    })
    .await?;

    rx.await?
}

pub(crate) async fn cancel_consume(mgr: &QueueManagerSink, queue_name: &str, consumer_tag: &str) -> Result<()> {
    let (tx, rx) = oneshot::channel();

    chk!(
        mgr.send(QueueManagerCommand::CancelConsume {
            queue_name: queue_name.to_string(),
            consumer_tag: consumer_tag.to_string(),
            result: tx,
        })
        .await
    )?;

    rx.await?
}

pub(crate) async fn get_command_sink(mgr: &QueueManagerSink, queue_name: &str) -> Result<QueueCommandSink> {
    let (tx, rx) = oneshot::channel();

    mgr.send(QueueManagerCommand::GetQueueSink {
        queue_name: queue_name.to_string(),
        result: tx,
    })
    .await?;

    rx.await?
}

async fn command_loop(mut stream: mpsc::Receiver<QueueManagerCommand>) -> Result<()> {
    use QueueManagerCommand::*;

    let mut queues = HashMap::<String, QueueState>::new();

    while let Some(command) = stream.recv().await {
        debug!("Manager command {:?}", command);

        match command {
            Declare { queue, conn_id, result } => {
                logerr!(result.send(handle_declare(&mut queues, queue, conn_id).await));
            }
            Consume {
                conn_id,
                queue_name,
                consumer_tag,
                no_ack,
                outgoing,
                result,
            } => {
                logerr!(
                    result.send(handle_consume(&queues, &conn_id, &queue_name, &consumer_tag, no_ack, outgoing).await)
                );
            }
            CancelConsume {
                queue_name,
                consumer_tag,
                result,
            } => {
                match handle_cancel(&queues, &queue_name, &consumer_tag).await {
                    Ok(still_alive) => {
                        logerr!(result.send(Ok(())));

                        if !still_alive {
                            // Queue is auto delete and the last consumer has cancelled.
                            break;
                        }
                    }
                    Err(e) => {
                        error!("Error {:?}", e);

                        logerr!(result.send(Ok(())));
                    }
                }
            }
            GetQueueSink { queue_name, result } => {
                logerr!(result.send(handle_get_command_sink(&queues, &queue_name)));
            }
        }
    }

    Ok(())
}

/// Declare queue with the given parameters. Declare means if the queue hasn't existed yet, it
/// creates that.
async fn handle_declare(queues: &mut HashMap<String, QueueState>, queue: Queue, conn_id: String) -> Result<()> {
    // TODO implement different queue properties (exclusive, auto-delete, durable, properties)
    match queues.get(&queue.name) {
        Some(_) => Ok(()),
        None => {
            let (cmd_tx, mut cmd_rx) = mpsc::channel(1);
            let queue_name = queue.name.clone();
            let queue_state = QueueState {
                queue: queue.clone(),
                command_sink: cmd_tx.clone(),
            };

            tokio::spawn(async move {
                handler::start(queue, conn_id, &mut cmd_rx).await;
            });

            queues.insert(queue_name.to_string(), queue_state);

            Ok(())
        }
    }
}

fn handle_get_command_sink(queues: &HashMap<String, QueueState>, name: &str) -> Result<QueueCommandSink> {
    match queues.get(name) {
        Some(queue) => Ok(queue.command_sink.clone()),
        None => channel_error(0, frame::QUEUE_DECLARE, ChannelError::NotFound, "Not found"),
    }
}

async fn handle_consume(
    queues: &HashMap<String, QueueState>,
    conn_id: &str,
    name: &str,
    consumer_tag: &str,
    no_ack: bool,
    outgoing: mpsc::Sender<AMQPFrame>,
) -> Result<QueueCommandSink> {
    match queues.get(name) {
        Some(queue) => {
            let (tx, rx) = oneshot::channel();

            queue
                .command_sink
                .send_timeout(
                    QueueCommand::StartConsuming {
                        conn_id: conn_id.to_string(),
                        consumer_tag: consumer_tag.to_string(),
                        no_ack,
                        sink: outgoing,
                        result: tx,
                    },
                    time::Duration::from_secs(1),
                )
                .await?;

            logerr!(rx.await?);

            Ok(queue.command_sink.clone())
        }
        None => channel_error(0, frame::BASIC_CONSUME, ChannelError::NotFound, "Not found"),
    }
}

async fn handle_cancel(queues: &HashMap<String, QueueState>, name: &str, consumer_tag: &str) -> Result<bool> {
    match queues.get(name) {
        Some(queue) => {
            let (tx, rx) = oneshot::channel();

            queue
                .command_sink
                .send_timeout(
                    QueueCommand::CancelConsuming {
                        consumer_tag: consumer_tag.to_string(),
                        result: tx,
                    },
                    time::Duration::from_secs(1),
                )
                .await?;

            Ok(rx.await?)
        }
        None => Ok(true),
    }
}
