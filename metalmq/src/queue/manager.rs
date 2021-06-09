use crate::client::{channel_error, ChannelError};
use crate::queue::consumer_handler::{self, ConsumerCommand};
use crate::queue::handler::{self, QueueCommandSink};
use crate::queue::Queue;
use crate::Result;
use log::error;
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

pub(crate) enum QueueManagerCommand {
    Declare {
        name: String,
        result: oneshot::Sender<Result<()>>,
    },
    Consume {
        queue_name: String,
        consumer_tag: String,
        no_ack: bool,
        outgoing: mpsc::Sender<AMQPFrame>,
        result: oneshot::Sender<Result<()>>,
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

pub(crate) async fn declare_queue(mgr: &QueueManagerSink, queue_name: &str) -> Result<()> {
    let (tx, rx) = oneshot::channel();

    mgr.send(QueueManagerCommand::Declare {
        name: queue_name.to_string(),
        result: tx,
    });

    rx.await?
}

pub(crate) async fn consume(
    mgr: &QueueManagerSink,
    queue_name: &str,
    consumer_tag: &str,
    no_ack: bool,
    outgoing: mpsc::Sender<AMQPFrame>,
) -> Result<()> {
    let (tx, rx) = oneshot::channel();

    mgr.send(QueueManagerCommand::Consume {
        queue_name: queue_name.to_string(),
        consumer_tag: consumer_tag.to_string(),
        no_ack,
        outgoing,
        result: tx,
    });

    rx.await?
}

pub(crate) async fn cancel_consume(mgr: &QueueManagerSink, queue_name: &str, consumer_tag: &str) -> Result<()> {
    let (tx, rx) = oneshot::channel();

    mgr.send(QueueManagerCommand::CancelConsume {
        queue_name: queue_name.to_string(),
        consumer_tag: consumer_tag.to_string(),
        result: tx,
    });

    rx.await?
}

pub(crate) async fn get_command_sink(mgr: &QueueManagerSink, queue_name: &str) -> Result<QueueCommandSink> {
    let (tx, rx) = oneshot::channel();

    mgr.send(QueueManagerCommand::GetQueueSink {
        queue_name: queue_name.to_string(),
        result: tx,
    });

    rx.await?
}

async fn command_loop(mut stream: mpsc::Receiver<QueueManagerCommand>) -> Result<()> {
    let mut queues = HashMap::<String, Queue>::new();

    while let Some(command) = stream.recv().await {
        handle_command(&mut queues, command);
    }

    Ok(())
}

async fn handle_command(mut queues: &mut HashMap<String, Queue>, command: QueueManagerCommand) {
    use QueueManagerCommand::*;

    match command {
        Declare { name, result } => {
            result.send(handle_declare(&mut queues, &name).await);
        }
        Consume {
            queue_name,
            consumer_tag,
            no_ack,
            outgoing,
            result,
        } => {
            result.send(handle_consume(&queues, &queue_name, &consumer_tag, no_ack, outgoing).await);
        }
        CancelConsume {
            queue_name,
            consumer_tag,
            result,
        } => {
            result.send(handle_cancel(&queues, &queue_name, &consumer_tag).await);
        }
        GetQueueSink { queue_name, result } => {
            result.send(handle_get_command_sink(&queues, &queue_name));
        }
    }
}

/// Declare queue with the given parameters. Declare means if the queue hasn't existed yet, it
/// creates that.
async fn handle_declare(queues: &mut HashMap<String, Queue>, name: &str) -> Result<()> {
    // TODO implement different queue properties (exclusive, auto-delete, durable, properties)
    match queues.get(name) {
        Some(queue) => Ok(()),
        None => {
            let (cmd_tx, mut cmd_rx) = mpsc::channel(1);
            let (cons_tx, mut cons_rx) = mpsc::channel(1);

            let queue = Queue {
                name: name.to_string(),
                command_sink: cmd_tx.clone(),
                consumer_sink: cons_tx.clone(),
            };

            tokio::spawn(async move {
                handler::queue_loop(&mut cmd_rx, cons_tx).await;
            });

            tokio::spawn(async move {
                consumer_handler::consumer_handler_loop(&mut cons_rx, cmd_tx).await;
            });

            queues.insert(name.to_string(), queue);

            Ok(())
        }
    }
}

fn handle_get_command_sink(queues: &HashMap<String, Queue>, name: &str) -> Result<QueueCommandSink> {
    match queues.get(name) {
        Some(queue) => Ok(queue.command_sink.clone()),
        None => channel_error(0, frame::QUEUE_DECLARE, ChannelError::NotFound, "Not found"),
    }
}

async fn handle_consume(
    queues: &HashMap<String, Queue>,
    name: &str,
    consumer_tag: &str,
    no_ack: bool,
    outgoing: mpsc::Sender<AMQPFrame>,
) -> Result<()> {
    match queues.get(name) {
        Some(queue) => {
            let (tx, rx) = oneshot::channel();

            queue
                .consumer_sink
                .send_timeout(
                    ConsumerCommand::StartConsuming {
                        consumer_tag: consumer_tag.to_string(),
                        no_ack,
                        sink: outgoing,
                        result: tx,
                    },
                    time::Duration::from_secs(1),
                )
                .await?;

            rx.await?;

            Ok(())
        }
        None => channel_error(0, frame::BASIC_CONSUME, ChannelError::NotFound, "Not found"),
    }
}

async fn handle_cancel(queues: &HashMap<String, Queue>, name: &str, consumer_tag: &str) -> Result<()> {
    match queues.get(name) {
        Some(queue) => {
            let (tx, rx) = oneshot::channel();

            queue
                .consumer_sink
                .send_timeout(
                    ConsumerCommand::CancelConsuming {
                        consumer_tag: consumer_tag.to_string(),
                        result: tx,
                    },
                    time::Duration::from_secs(1),
                )
                .await?;

            rx.await?;

            Ok(())
        }
        None => Ok(()),
    }
}
