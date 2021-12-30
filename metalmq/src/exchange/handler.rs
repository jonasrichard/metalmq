use crate::client;
use crate::exchange::{Exchange, ExchangeType};
use crate::message::{self, Message};
use crate::queue::handler::{QueueCommand, QueueCommandSink};
use crate::{logerr, send, Result};
use log::{error, info, trace};
use metalmq_codec::codec::Frame;
use metalmq_codec::frame;
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

pub type ExchangeCommandSink = mpsc::Sender<ExchangeCommand>;

#[derive(Debug)]
pub enum MessageSentResult {
    None,
    MessageNotRouted(Message),
}

#[derive(Debug)]
pub enum ExchangeCommand {
    Message {
        message: Message,
        outgoing: mpsc::Sender<Frame>,
    },
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
    Delete {
        channel: u16,
        if_unused: bool,
        result: oneshot::Sender<Result<()>>,
    },
}

enum Binding {
    Direct { routing_key: String, queue_name: String },
    Fanout { queue_name: String },
}

struct ExchangeState {
    exchange: super::Exchange,
    /// Queue bindings
    queues: Vec<(Binding, QueueCommandSink)>,
}

pub async fn start(exchange: Exchange, commands: &mut mpsc::Receiver<ExchangeCommand>) {
    ExchangeState {
        exchange,
        queues: vec![],
    }
    .exchange_loop(commands)
    .await
    .unwrap();
}

impl ExchangeState {
    pub async fn exchange_loop(&mut self, commands: &mut mpsc::Receiver<ExchangeCommand>) -> Result<()> {
        while let Some(command) = commands.recv().await {
            trace!("Command {:?}", command);

            if !self.handle_command(command).await.unwrap() {
                break;
            }
        }

        Ok(())
    }

    pub async fn handle_command(&mut self, command: ExchangeCommand) -> Result<bool> {
        match command {
            ExchangeCommand::Message { message, outgoing } => {
                if let Some(failed_message) = self.route_message(message).await? {
                    if failed_message.mandatory {
                        message::send_basic_return(failed_message, &outgoing).await?;
                    }
                }

                Ok(true)
                //Some(queue) => {
                //    // FIXME we can close a message as far as we don't use Vec but Bytes.
                //    // Vec is cloned by cloning the underlying array, but Buffer is a bit
                //    // more specialized, and it uses a reference counter pointer.
                //    info!("Publish message {:?}", message);

                //    if let Err(e) = send!(queue, QueueCommand::PublishMessage(message)) {
                //        error!("Send error {:?}", e);
                //    }
                //}
            }
            ExchangeCommand::QueueBind {
                queue_name,
                routing_key,
                sink,
                result,
            } => {
                match self.exchange.exchange_type {
                    ExchangeType::Direct => {
                        let binding = Binding::Direct {
                            routing_key,
                            queue_name,
                        };
                        self.queues.push((binding, sink.clone()));
                        logerr!(send!(
                            sink,
                            QueueCommand::ExchangeBound {
                                exchange_name: self.exchange.name.clone(),
                            }
                        ));
                        logerr!(result.send(true));
                    }
                    ExchangeType::Fanout => {
                        self.queues.push((Binding::Fanout { queue_name }, sink.clone()));
                        logerr!(send!(
                            sink,
                            QueueCommand::ExchangeBound {
                                exchange_name: self.exchange.name.clone(),
                            }
                        ));
                        logerr!(result.send(true));
                    }
                    _ => {
                        logerr!(result.send(false));
                    }
                }

                Ok(true)
            }
            ExchangeCommand::QueueUnbind {
                queue_name,
                routing_key,
                result,
            } => {
                match self.exchange.exchange_type {
                    ExchangeType::Direct => {
                        if let Some(pos) = self.queues.iter().position(|b| match &b.0 {
                            Binding::Direct {
                                routing_key: rk,
                                queue_name: qn,
                            } => &routing_key == rk && &queue_name == qn,
                            _ => false,
                        }) {
                            let binding = self.queues.remove(pos);
                            logerr!(send!(
                                binding.1,
                                QueueCommand::ExchangeUnbound {
                                    exchange_name: self.exchange.name.clone(),
                                }
                            ));
                        }
                        logerr!(result.send(true));
                    }
                    ExchangeType::Fanout => {
                        if let Some(pos) = self.queues.iter().position(|b| match &b.0 {
                            Binding::Fanout { queue_name: qn } => &queue_name == qn,
                            _ => false,
                        }) {
                            let binding = self.queues.remove(pos);
                            logerr!(send!(
                                binding.1,
                                QueueCommand::ExchangeUnbound {
                                    exchange_name: self.exchange.name.clone(),
                                }
                            ));
                        }
                        logerr!(result.send(true));
                    }
                    _ => {
                        result.send(false);
                    }
                }

                Ok(true)
            }
            ExchangeCommand::Delete {
                channel,
                if_unused,
                result,
            } => {
                if if_unused {
                    if self.queues.is_empty() {
                        result.send(Ok(()));

                        Ok(false)
                    } else {
                        let err = client::channel_error(
                            channel,
                            frame::EXCHANGE_DELETE,
                            client::ChannelError::PreconditionFailed,
                            "Exchange is in use",
                        );

                        result.send(err);

                        Ok(true)
                    }
                } else {
                    for q in &self.queues {
                        let cmd = QueueCommand::ExchangeUnbound {
                            exchange_name: self.exchange.name.clone(),
                        };

                        logerr!(q.1.send(cmd).await);
                    }

                    self.queues.clear();

                    logerr!(result.send(Ok(())));

                    Ok(false)
                }
            }
        }
    }

    /// Route the message according to the exchange type and the bindings. If there is no queue to
    /// be send the message to, it gives back the messages in the Option.
    async fn route_message(&self, message: Message) -> Result<Option<Message>> {
        match self.exchange.exchange_type {
            ExchangeType::Direct => {
                for binding in &self.queues {
                    match &binding.0 {
                        // If the exchange type is direct we need to check all the direct bindings
                        // and send messages if routing key is exactly the same.
                        Binding::Direct { routing_key, .. } if routing_key == &message.routing_key => {
                            let cmd = QueueCommand::PublishMessage(message.clone());
                            logerr!(binding.1.send(cmd).await);
                        }
                        // Other bindings are ignored now.
                        _ => (),
                    }
                }

                // FIXME if there are no match we need to send back the message (because it was
                // unroutable)
                Ok(None)
            }
            ExchangeType::Fanout => {
                for binding in &self.queues {
                    match &binding.0 {
                        Binding::Fanout { .. } => {
                            let cmd = QueueCommand::PublishMessage(message.clone());
                            logerr!(binding.1.send(cmd).await);
                        }
                        _ => (),
                    }
                }

                // FIXME same as in the other branch
                Ok(None)
            }
            _ => Ok(Some(message)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metalmq_codec::frame;

    async fn recv_timeout(rx: &mut mpsc::Receiver<Frame>) -> Option<Frame> {
        let sleep = tokio::time::sleep(tokio::time::Duration::from_secs(1));
        tokio::pin!(sleep);

        tokio::select! {
            frame = rx.recv() => {
                frame
            }
            _ = &mut sleep => {
                return None;
            }
        }
    }

    #[tokio::test]
    async fn send_basic_return_on_mandatory_unroutable_message() {
        let (msg_tx, mut msg_rx) = mpsc::channel(1);
        let mut es = ExchangeState {
            exchange: Exchange {
                name: "x-name".to_string(),
                exchange_type: ExchangeType::Direct,
                durable: false,
                auto_delete: false,
                internal: false,
            },
            queues: vec![],
        };

        let msg = Message {
            source_connection: "conn-id".to_string(),
            channel: 2,
            content: b"Hello".to_vec(),
            exchange: "x-name".to_string(),
            routing_key: "".to_string(),
            mandatory: true,
            immediate: false,
        };
        let cmd = ExchangeCommand::Message(msg);

        let res = es.handle_command(cmd).await;
        assert!(res.is_ok());

        match recv_timeout(&mut msg_rx).await {
            Some(Frame::Frame(br)) => assert!(true),
            Some(Frame::Frames(fs)) => if let frame::AMQPFrame::Method(ch, cm, args) = fs.get(0).unwrap() {},
            None => assert!(false, "Basic.Return frame is expected"),
        }
    }
}
