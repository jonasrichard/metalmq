use crate::Result;
use crate::message::Message;
use crate::queue::handler::{QueueCommandSink, QueueCommand};
use std::collections::HashMap;
use log::{debug, error};
use tokio::sync::{mpsc, oneshot};

pub(crate) type ControlChannel = mpsc::Sender<ManagerCommand>;
pub(crate) type ExchangeChannel = mpsc::Sender<ExchangeCommand>;

#[derive(Debug)]
pub(crate) enum ManagerCommand {
    ExchangeClone { name: String, connection_id: String, response: oneshot::Sender<ExchangeChannel> },
    QueueBind { exchange_name: String, sink: QueueCommandSink }
}

#[derive(Debug)]
pub(crate) enum ExchangeCommand {
    Message(Message),
    QueueBind { sink: QueueCommandSink }
}

struct Exchanges {
    control: ExchangeChannel,
    /// Connections which are currently using this exchange
    connections: Vec<String>
}

pub(crate) async fn exchange_manager_loop(control: &mut mpsc::Receiver<ManagerCommand>) -> Result<()> {
    let mut exchanges = HashMap::<String, Exchanges>::new();

    while let Some(command) = control.recv().await {
        debug!("{:?}", command);

        match command {
            ManagerCommand::ExchangeClone{ name, connection_id, response } => {
                if let Some(ex) = exchanges.get_mut(&name) {
                    if !ex.connections.contains(&connection_id) {
                        ex.connections.push(connection_id)
                    }

                    if let Err(e) = response.send(ex.control.clone()) {
                        error!("Send error {:?}", e);
                    }
                } else {
                    let (tx, mut rx) = mpsc::channel(1);

                    tokio::spawn(async move {
                        if let Err(e) = exchange_loop(&mut rx).await {
                            error!("Exchange loop finish in {:?}", e);
                        }
                    });

                    let control = tx.clone();    // TODO maintain count
                    if let Err(e) = response.send(tx) {
                        error!("Send error {:?}", e);
                    }

                    exchanges.insert(name, Exchanges {
                        control: control,
                        connections: vec![connection_id]
                    });
                }
            },
            ManagerCommand::QueueBind{ exchange_name, sink } => {
                if let Some(ex) = exchanges.get(&exchange_name) {
                    if let Err(e) = ex.control.send(ExchangeCommand::QueueBind{ sink: sink }).await {
                        error!("Send error {:?}", e);
                    }
                }
            }
        }
    }

    Ok(())
}

pub(crate) async fn exchange_loop(commands: &mut mpsc::Receiver<ExchangeCommand>) -> Result<()> {
    let mut queues = Vec::<QueueCommandSink>::new();

    while let Some(command) = commands.recv().await {
        debug!("{:?}", command);

        match command {
            ExchangeCommand::Message(message) =>
                for ch in &queues {
                    if let Err(e) = ch.send(QueueCommand::Message(message.clone())).await {
                        error!("Send error {:?}", e);
                    }
                },
            ExchangeCommand::QueueBind{ sink } =>
                queues.push(sink)
        }
    }

    Ok(())
}
