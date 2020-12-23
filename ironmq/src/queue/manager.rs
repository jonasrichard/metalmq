use crate::Result;
use crate::message::Message;
use crate::queue::Queue;
use crate::queue::handler::{self, FrameChannel, QueueChannel, ManagerCommand};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};

pub(crate) struct Queues {
    control: mpsc::Sender<handler::ManagerCommand>,
    queues: HashMap<String, Queue>
}

// TODO in exchange manager we need to introduce a bind_queue fn
#[async_trait]
pub(crate) trait QueueManager {
    async fn declare(&mut self, name: String) -> Result<QueueChannel>;
    async fn get_channel(&mut self, name: String) -> Result<QueueChannel>;
    async fn consume(&mut self, name: String, out: FrameChannel) -> Result<()>;
}

pub(crate) fn start() -> Queues {
    let (sink, mut source) = mpsc::channel(1);

    tokio::spawn(async move {
        handler::queue_manager_loop(&mut source).await;
    });

    Queues {
        control: sink,
        queues: HashMap::new()
    }
}

#[async_trait]
impl QueueManager for Queues {
    async fn declare(&mut self, name: String) -> Result<QueueChannel> {
        let (tx, rx) = oneshot::channel();
        self.control.send(ManagerCommand::QueueClone { name: name, clone: tx }).await?;
        let ch = rx.await?;

        Ok(ch)
    }

    async fn get_channel(&mut self, name: String) -> Result<QueueChannel> {
        let (tx, rx) = oneshot::channel();
        self.control.send(ManagerCommand::QueueClone { name: name, clone: tx }).await?;
        let ch = rx.await?;

        Ok(ch)
    }

    async fn consume(&mut self, name: String, out: FrameChannel) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.control.send(ManagerCommand::Consume { queue_name: name, sink: out}).await?;

        Ok(rx.await?)
    }
}
