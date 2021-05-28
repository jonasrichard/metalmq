use crate::message::Message;
use crate::queue::handler::{QueueCommand, QueueCommandSink};
use crate::Result;
use log::{debug, error};
use tokio::sync::{mpsc, oneshot};

pub(crate) type ExchangeCommandSink = mpsc::Sender<ExchangeCommand>;

#[derive(Debug)]
pub(crate) enum MessageSentResult {
    None,
    MessageNotRouted(Message),
}

#[derive(Debug)]
pub(crate) enum ExchangeCommand {
    Message(Message, oneshot::Sender<MessageSentResult>),
    QueueBind { sink: QueueCommandSink },
}

pub(crate) async fn exchange_loop(commands: &mut mpsc::Receiver<ExchangeCommand>) -> Result<()> {
    let mut queues = Vec::<QueueCommandSink>::new();

    while let Some(command) = commands.recv().await {
        match command {
            ExchangeCommand::Message(message, result) => {
                if queues.len() == 0 {
                    if let Err(e) = result.send(MessageSentResult::MessageNotRouted(message)) {
                        error!("Error sending message back {:?}", e);
                    }
                } else {
                    // TODO here we need to check if this exchange is bound to a queue, or
                    // if routing key will send this message to a queue.
                    //   If not, we need to check if the message is mandatory, we need to
                    //   send back a basic-return with an error.
                    debug!(
                        "Publish message {}",
                        String::from_utf8(message.content.clone()).unwrap()
                    );

                    for ch in &queues {
                        if let Err(e) = ch.send(QueueCommand::PublishMessage(message.clone())).await {
                            error!("Send error {:?}", e);
                        }
                    }

                    if let Err(e) = result.send(MessageSentResult::None) {
                        error!("Error sending message back {:?}", e);
                    }
                }
            }
            ExchangeCommand::QueueBind { sink } => queues.push(sink),
        }
    }

    Ok(())
}
