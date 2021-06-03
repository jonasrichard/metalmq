use crate::client::{channel_error, ChannelError};
use crate::queue::consumer_handler::{self, ConsumerCommand, ConsumerCommandSink};
use crate::queue::handler::{self, QueueCommandSink};
use crate::queue::Queue;
use crate::Result;
use metalmq_codec::frame::{self, AMQPFrame};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};
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
pub(crate) struct QueueManager {
    queues: Arc<Mutex<HashMap<String, Queue>>>,
}

pub(crate) fn start() -> QueueManager {
    QueueManager {
        queues: Arc::new(Mutex::new(HashMap::new())),
    }
}

// TODO in exchange manager we need to introduce a bind_queue fn
impl QueueManager {
    /// Declare queue with the given parameters. Declare means if the queue hasn't existed yet, it
    /// creates that.
    pub(crate) async fn declare(&mut self, name: String) -> Result<()> {
        let mut q = self.queues.lock().await;

        // TODO implement different queue properties (exclusive, auto-delete, durable, properties)
        match q.get(&name) {
            Some(queue) => Ok(()),
            None => {
                let (cmd_tx, mut cmd_rx) = mpsc::channel(1);
                let (cons_tx, mut cons_rx) = mpsc::channel(1);

                let queue = Queue {
                    name: name.clone(),
                    command_sink: cmd_tx.clone(),
                    consumer_sink: cons_tx.clone(),
                };

                tokio::spawn(async move {
                    handler::queue_loop(&mut cmd_rx, cons_tx).await;
                });

                tokio::spawn(async move {
                    consumer_handler::consumer_handler_loop(&mut cons_rx, cmd_tx).await;
                });

                q.insert(name, queue);

                Ok(())
            }
        }
    }

    pub(crate) async fn get_command_sink(&mut self, name: &str) -> Result<QueueCommandSink> {
        let q = self.queues.lock().await;

        match q.get(name) {
            Some(queue) => Ok(queue.command_sink.clone()),
            None => channel_error(0, frame::QUEUE_DECLARE, ChannelError::NotFound, "Not found"),
        }
    }

    pub(crate) async fn consume(
        &mut self,
        name: String,
        consumer_tag: String,
        no_ack: bool,
        outgoing: mpsc::Sender<AMQPFrame>,
    ) -> Result<()> {
        let q = self.queues.lock().await;

        match q.get(&name) {
            Some(queue) => {
                let (tx, rx) = oneshot::channel();

                queue
                    .consumer_sink
                    .send_timeout(
                        ConsumerCommand::StartConsuming {
                            consumer_tag,
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

    pub(crate) async fn cancel(&mut self, name: String, consumer_tag: String) -> Result<()> {
        let q = self.queues.lock().await;

        match q.get(&name) {
            Some(queue) => {
                let (tx, rx) = oneshot::channel();

                queue
                    .consumer_sink
                    .send_timeout(
                        ConsumerCommand::CancelConsuming {
                            consumer_tag,
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
}
