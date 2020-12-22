use crate::Result;
use crate::message::Message;
use crate::queue::handler::{QueueChannel, QueueCommand};
use std::collections::HashMap;
use log::debug;
use tokio::sync::{mpsc, oneshot};

pub(crate) type ControlChannel = mpsc::Sender<ManagerCommand>;
pub(crate) type ExchangeChannel = mpsc::Sender<ExchangeCommand>;

#[derive(Debug)]
pub(crate) enum ManagerCommand {
    ExchangeClone { name: String, clone: oneshot::Sender<ExchangeChannel> },
    QueueBind { exchange_name: String, sink: QueueChannel }
}

#[derive(Debug)]
pub(crate) enum ExchangeCommand {
    Message(Message),
    QueueBind { sink: QueueChannel }
}

pub(crate) async fn exchange_manager_loop(control: &mut mpsc::Receiver<ManagerCommand>) -> Result<()> {
    let mut exchanges = HashMap::<String, ExchangeChannel>::new();

    while let Some(command) = control.recv().await {
        debug!("{:?}", command);

        match command {
            ManagerCommand::ExchangeClone{ name, clone } => {
                if let Some(ex) = exchanges.get(&name) {
                    clone.send(ex.clone());
                } else {
                    let (tx, mut rx) = mpsc::channel(1);

                    tokio::spawn(async move {
                        exchange_loop(&mut rx).await;
                    });

                    let result = tx.clone();    // TODO maintain count
                    clone.send(tx);

                    exchanges.insert(name, result);
                }
            },
            ManagerCommand::QueueBind{ exchange_name, sink } => {
                if let Some(ex) = exchanges.get(&exchange_name) {
                    ex.send(ExchangeCommand::QueueBind{ sink: sink }).await;
                }
            },
            _ =>
                ()
        }
    }

    Ok(())
}

pub(crate) async fn exchange_loop(commands: &mut mpsc::Receiver<ExchangeCommand>) -> Result<()> {
    let mut queues = Vec::<QueueChannel>::new();

    while let Some(command) = commands.recv().await {
        debug!("{:?}", command);

        match command {
            ExchangeCommand::Message(message) =>
                for ch in &queues {
                    ch.send(QueueCommand::Message(message.clone())).await;
                },
            ExchangeCommand::QueueBind{ sink } =>
                queues.push(sink)
        }
    }

    Ok(())
}
