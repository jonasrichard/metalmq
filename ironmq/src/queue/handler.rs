use crate::Result;
use crate::message::{Message, MessageChannel};
use log::debug;
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

pub(crate) type ControlChannel = mpsc::Sender<ManagerCommand>;
pub(crate) type QueueChannel = mpsc::Sender<QueueCommand>;

#[derive(Debug)]
pub(crate) enum ManagerCommand {
    QueueClone { name: String, clone: oneshot::Sender<QueueChannel> },
    Consume { queue_name: String, sink: MessageChannel }
}

#[derive(Debug)]
pub(crate) enum QueueCommand {
    Message(Message),
    Consume{ sink: MessageChannel }
}

pub(crate) async fn queue_manager_loop(control: &mut mpsc::Receiver<ManagerCommand>) -> Result<()> {
    let mut queues = HashMap::<String, QueueChannel>::new();

    while let Some(command) = control.recv().await {
        debug!("{:?}", command);

        match command {
            ManagerCommand::QueueClone{ name, clone } => {
                if let Some(queue) = queues.get(&name) {
                    clone.send(queue.clone());
                } else {
                    let (tx, mut rx) = mpsc::channel(1);

                    tokio::spawn(async move {
                        queue_loop(&mut rx).await;
                    });

                    let result = tx.clone();
                    clone.send(tx);

                    queues.insert(name, result);
                }
            },
            ManagerCommand::Consume{ queue_name, sink } => {
                if let Some(queue) = queues.get(&queue_name) {
                    queue.send(QueueCommand::Consume{ sink: sink });
                }
            },
            _ =>
                ()
        }
    }

    Ok(())
}

pub(crate) async fn queue_loop(commands: &mut mpsc::Receiver<QueueCommand>) {
    let mut consumers = Vec::<MessageChannel>::new();

    while let Some(command) = commands.recv().await {
        debug!("Queue got message: {:?}", command);

        match command {
            QueueCommand::Message(message) =>
                for ch in &consumers {
                    ch.send(message.clone()).await;
                },
            QueueCommand::Consume{ sink } =>
                consumers.push(sink)
        }
    }
}
