use crate::exchange::Exchange;
use crate::message::Message;
use crate::queue::handler::{QueueCommand, QueueCommandSink};
use crate::{logerr, send, Result};
use log::{error, info, trace};
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

struct ExchangeState {
    exchange: super::Exchange,
    queues: HashMap<String, QueueCommandSink>,
}

pub(crate) async fn start(exchange: Exchange, commands: &mut mpsc::Receiver<ExchangeCommand>) {
    ExchangeState {
        exchange,
        queues: HashMap::new(),
    }
    .exchange_loop(commands)
    .await;
}

impl ExchangeState {
    pub(crate) async fn exchange_loop(&mut self, commands: &mut mpsc::Receiver<ExchangeCommand>) -> Result<()> {
        while let Some(command) = commands.recv().await {
            trace!("Command {:?}", command);

            self.handle_command(command).await;
        }

        Ok(())
    }

    pub(crate) async fn handle_command(&mut self, command: ExchangeCommand) -> Result<()> {
        match command {
            ExchangeCommand::Message(message, result) => {
                match self.choose_queue_by_routing_key(&message.routing_key) {
                    Some(queue) => {
                        // TODO here we need to check if this exchange is bound to a queue, or
                        // if routing key will send this message to a queue.
                        //   If not, we need to check if the message is mandatory, we need to
                        //   send back a basic-return with an error.
                        info!(
                            "Publish message {}",
                            String::from_utf8(message.content.clone()).unwrap()
                        );

                        // FIXME this causes deadlock
                        if let Err(e) = send!(queue, QueueCommand::PublishMessage(message.clone())) {
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
                self.queues.insert(routing_key, sink.clone());
                logerr!(send!(
                    sink,
                    QueueCommand::ExchangeBound {
                        exchange_name: self.exchange.name.clone(),
                    }
                ));
                logerr!(result.send(true));
            }
            ExchangeCommand::QueueUnbind {
                queue_name,
                routing_key,
                result,
            } => {
                if let Some(sink) = self.queues.remove(&routing_key) {
                    logerr!(send!(
                        sink,
                        QueueCommand::ExchangeUnbound {
                            exchange_name: self.exchange.name.clone(),
                        }
                    ));
                }

                logerr!(result.send(true));
            }
        }

        Ok(())
    }

    fn choose_queue_by_routing_key(&self, routing_key: &str) -> Option<&QueueCommandSink> {
        self.queues.get(routing_key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn send_basic_return_on_mandatory_unroutable_message() {
        let mut es = ExchangeState {
            exchange: Exchange {
                name: "x-name".to_string(),
                exchange_type: "direct".to_string(),
                durable: false,
                auto_delete: false,
                internal: false,
            },
            queues: HashMap::new(),
        };

        let (tx, rx) = oneshot::channel();
        let msg = Message {
            source_connection: "conn-id".to_string(),
            channel: 2,
            content: b"Hello".to_vec(),
            exchange: "x-name".to_string(),
            routing_key: "".to_string(),
            mandatory: true,
            immediate: false,
        };
        let cmd = ExchangeCommand::Message(msg, tx);

        let res = es.handle_command(cmd).await;
        assert!(res.is_ok());

        // TODO how basic return will be sent? We need to pass here the outgoing sink
    }
}
