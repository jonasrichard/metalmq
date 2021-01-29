use crate::Result;
use crate::client::error;
use crate::queue::Queue;
use crate::queue::handler::{self, QueueCommand, QueueCommandSink};
use std::collections::HashMap;
use ironmq_codec::frame::{self, AMQPFrame};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};

pub(crate) struct Queues {
    queues: Arc<Mutex<HashMap<String, Queue>>>
}

// TODO in exchange manager we need to introduce a bind_queue fn
#[async_trait]
pub(crate) trait QueueManager {
    /// Declare queue with the given parameters. Declare means if the queue hasn't existed yet, it
    /// creates that.
    async fn declare(&mut self, name: String) -> Result<QueueCommandSink>;
    async fn get_channel(&mut self, name: String) -> Result<QueueCommandSink>;
    async fn consume(&mut self, name: String, outgoing: mpsc::Sender<AMQPFrame>) -> Result<()>;
}

pub(crate) fn start() -> Queues {
    Queues {
        queues: Arc::new(Mutex::new(HashMap::new()))
    }
}

#[async_trait]
impl QueueManager for Queues {
    async fn declare(&mut self, name: String) -> Result<QueueCommandSink> {
        let mut q = self.queues.lock().await;

        match q.get(&name) {
            Some(queue) =>
                Ok(queue.command_sink.clone()),
            None => {
                let (cmd_tx, mut cmd_rx) = mpsc::channel(1);

                let queue = Queue {
                    name: name.clone(),
                    command_sink: cmd_tx.clone()
                };

                tokio::spawn(async move {
                    handler::queue_loop(&mut cmd_rx).await;
                });

                q.insert(name, queue);

                Ok(cmd_tx)
            }
        }
    }

    async fn get_channel(&mut self, name: String) -> Result<QueueCommandSink> {
        let q = self.queues.lock().await;

        match q.get(&name) {
            Some(queue) =>
                Ok(queue.command_sink.clone()),
            None =>
                // TODO check error code because we can call this from several places
                error(0, frame::QUEUE_DECLARE, 404, "Not found")
        }
    }

    async fn consume(&mut self, name: String, outgoing: mpsc::Sender<AMQPFrame>) -> Result<()> {
        let q = self.queues.lock().await;

        match q.get(&name) {
            Some(queue) => {
                let (tx, rx) = oneshot::channel();
                queue.command_sink.send(QueueCommand::Consume { frame_sink: outgoing, response: tx }).await?;

                rx.await?;

                Ok(())
            },
            None =>
                error(0, frame::BASIC_CONSUME, 404, "Not found")
        }
    }
}
