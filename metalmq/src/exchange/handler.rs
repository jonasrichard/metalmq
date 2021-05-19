use crate::message::Message;
use crate::queue::handler::{QueueCommand, QueueCommandSink};
use crate::Result;
use log::{debug, error};
use tokio::sync::mpsc;

pub(crate) type ExchangeCommandSink = mpsc::Sender<ExchangeCommand>;

#[derive(Debug)]
pub(crate) enum ExchangeCommand {
    Message(Message),
    QueueBind { sink: QueueCommandSink },
}

pub(crate) async fn exchange_loop(commands: &mut mpsc::Receiver<ExchangeCommand>) -> Result<()> {
    let mut queues = Vec::<QueueCommandSink>::new();

    while let Some(command) = commands.recv().await {
        match command {
            ExchangeCommand::Message(message) => {
                debug!(
                    "Publish message {}",
                    String::from_utf8(message.content.clone()).unwrap()
                );

                for ch in &queues {
                    if let Err(e) = ch.send(QueueCommand::Message(message.clone())).await {
                        error!("Send error {:?}", e);
                    }
                }
            }
            ExchangeCommand::QueueBind { sink } => queues.push(sink),
        }
    }

    Ok(())
}
