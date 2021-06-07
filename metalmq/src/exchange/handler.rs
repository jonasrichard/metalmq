use crate::message::Message;
use crate::queue::handler::{QueueCommand, QueueCommandSink};
use crate::Result;
use log::{debug, error};
use std::collections::HashMap;
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
    QueueBind {
        queue_name: String,
        routing_key: String,
        sink: QueueCommandSink,
        result: oneshot::Sender<bool>,
    },
    QueueUnbind {
        queue_name: String,
        routing_key: String,
        result: oneshot::Sender<bool>,
    },
}

pub(crate) async fn exchange_loop(
    exchange: super::Exchange,
    commands: &mut mpsc::Receiver<ExchangeCommand>,
) -> Result<()> {
    let mut queues = HashMap::<String, QueueCommandSink>::new();

    while let Some(command) = commands.recv().await {
        match command {
            ExchangeCommand::Message(message, result) => {
                match choose_queue_by_routing_key(&queues, &exchange.exchange_type, &message.routing_key) {
                    Some(queue) => {
                        // TODO here we need to check if this exchange is bound to a queue, or
                        // if routing key will send this message to a queue.
                        //   If not, we need to check if the message is mandatory, we need to
                        //   send back a basic-return with an error.
                        debug!(
                            "Publish message {}",
                            String::from_utf8(message.content.clone()).unwrap()
                        );

                        if let Err(e) = queue.send(QueueCommand::PublishMessage(message.clone())).await {
                            error!("Send error {:?}", e);
                        }

                        if let Err(e) = result.send(MessageSentResult::None) {
                            error!("Error sending message back {:?}", e);
                        }
                    }
                    None => {
                        if message.mandatory {
                            if let Err(e) = result.send(MessageSentResult::MessageNotRouted(message)) {
                                error!("Error sending message back {:?}", e);
                            }
                        }
                    }
                }
            }
            ExchangeCommand::QueueBind {
                queue_name,
                routing_key,
                sink,
                result,
            } => {
                queues.insert(routing_key, sink.clone());
                sink.send(QueueCommand::ExchangeBound {
                    exchange_name: exchange.name.clone(),
                })
                .await;
                // TODO make macro for error logged send
                result.send(true);
            }
            ExchangeCommand::QueueUnbind {
                queue_name,
                routing_key,
                result,
            } => {
                if let Some(sink) = queues.remove(&routing_key) {
                    sink.send(QueueCommand::ExchangeUnbound {
                        exchange_name: exchange.name.clone(),
                    })
                    .await;
                }

                result.send(true);
            }
        }
    }

    Ok(())
}

fn choose_queue_by_routing_key<'a>(
    queues: &'a HashMap<String, QueueCommandSink>,
    exchange_type: &str,
    routing_key: &str,
) -> Option<&'a QueueCommandSink> {
    queues.get(routing_key)
}
