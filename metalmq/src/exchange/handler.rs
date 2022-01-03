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

struct DirectBinding {
    routing_key: String,
    queue_name: String,
    queue: QueueCommandSink,
}

struct FanoutBinding {
    queue_name: String,
    queue: QueueCommandSink,
}

struct TopicBinding {
    routing_key: String,
    queue_name: String,
    queue: QueueCommandSink,
}

struct HeadersBinding {
    headers: HashMap<String, String>,
    queue_name: String,
    queue: QueueCommandSink,
}

enum Bindings {
    Direct(Vec<DirectBinding>),
    Fanout(Vec<FanoutBinding>),
    Topic(Vec<TopicBinding>),
    Headers(Vec<HeadersBinding>),
}

impl Bindings {
    fn add_direct_binding(&mut self, routing_key: String, queue_name: String, queue: QueueCommandSink) -> bool {
        if let Bindings::Direct(bs) = self {
            if let Some(_) = bs
                .iter()
                .position(|b| b.routing_key == routing_key && b.queue_name == queue_name)
            {
                // routing key and queue name are already bound
                return false;
            }

            bs.push(DirectBinding {
                routing_key,
                queue_name,
                queue,
            });

            return true;
        }

        false
    }

    fn remove_direct_binding(&mut self, routing_key: String, queue_name: String) -> Option<QueueCommandSink> {
        if let Bindings::Direct(bs) = self {
            if let Some(p) = bs
                .iter()
                .position(|b| b.routing_key == routing_key && b.queue_name == queue_name)
            {
                let binding = bs.remove(p);

                return Some(binding.queue);
            }
        }

        None
    }

    fn add_topic_binding(&mut self, routing_key: String, queue_name: String, queue: QueueCommandSink) -> bool {
        if let Bindings::Topic(bs) = self {
            if let Some(_) = bs
                .iter()
                .position(|b| b.routing_key == routing_key && b.queue_name == queue_name)
            {
                // routing key and queue name are already bound
                return false;
            }

            bs.push(TopicBinding {
                routing_key,
                queue_name,
                queue,
            });

            return true;
        }

        false
    }

    fn remove_topic_binding(&mut self, routing_key: String, queue_name: String) -> Option<QueueCommandSink> {
        if let Bindings::Topic(bs) = self {
            if let Some(p) = bs
                .iter()
                .position(|b| b.routing_key == routing_key && b.queue_name == queue_name)
            {
                let binding = bs.remove(p);

                return Some(binding.queue);
            }
        }

        None
    }

    fn add_fanout_binding(&mut self, queue_name: String, queue: QueueCommandSink) -> bool {
        if let Bindings::Fanout(bs) = self {
            if let Some(_) = bs.iter().position(|b| b.queue_name == queue_name) {
                return false;
            }

            bs.push(FanoutBinding { queue_name, queue });

            return true;
        }

        false
    }

    fn remove_fanout_binding(&mut self, queue_name: String) -> Option<QueueCommandSink> {
        if let Bindings::Fanout(bs) = self {
            if let Some(p) = bs.iter().position(|b| b.queue_name == queue_name) {
                let binding = bs.remove(p);

                return Some(binding.queue);
            }
        }

        return None;
    }

    fn is_empty(&self) -> bool {
        match self {
            Bindings::Direct(bs) => bs.is_empty(),
            Bindings::Topic(bs) => bs.is_empty(),
            Bindings::Fanout(bs) => bs.is_empty(),
            Bindings::Headers(bs) => bs.is_empty(),
        }
    }
}

struct ExchangeState {
    exchange: super::Exchange,
    /// Queue bindings
    bindings: Bindings,
}

pub async fn start(exchange: Exchange, commands: &mut mpsc::Receiver<ExchangeCommand>) {
    let et = exchange.exchange_type.clone();

    ExchangeState {
        exchange,
        bindings: match et {
            ExchangeType::Direct => Bindings::Direct(vec![]),
            ExchangeType::Fanout => Bindings::Fanout(vec![]),
            ExchangeType::Topic => Bindings::Topic(vec![]),
            ExchangeType::Headers => Bindings::Headers(vec![]),
        },
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
                let bind_result = match self.exchange.exchange_type {
                    ExchangeType::Direct => self.bindings.add_direct_binding(routing_key, queue_name, sink.clone()),
                    ExchangeType::Topic => self.bindings.add_topic_binding(routing_key, queue_name, sink.clone()),
                    ExchangeType::Fanout => self.bindings.add_fanout_binding(queue_name, sink.clone()),
                    _ => false,
                };

                if bind_result {
                    logerr!(send!(
                        sink,
                        QueueCommand::ExchangeBound {
                            exchange_name: self.exchange.name.clone(),
                        }
                    ));
                }

                logerr!(result.send(bind_result));

                Ok(true)
            }
            ExchangeCommand::QueueUnbind {
                queue_name,
                routing_key,
                result,
            } => {
                let sink = match self.exchange.exchange_type {
                    ExchangeType::Direct => self.bindings.remove_direct_binding(routing_key, queue_name),
                    ExchangeType::Topic => self.bindings.remove_topic_binding(routing_key, queue_name),
                    ExchangeType::Fanout => self.bindings.remove_fanout_binding(queue_name),
                    _ => None,
                };

                match sink {
                    Some(s) => {
                        logerr!(send!(
                            s,
                            QueueCommand::ExchangeUnbound {
                                exchange_name: self.exchange.name.clone(),
                            }
                        ));
                        result.send(true);
                    }
                    None => {
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
                    if self.bindings.is_empty() {
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
                    match &self.bindings {
                        Bindings::Direct(bs) => {
                            for b in bs {
                                let cmd = QueueCommand::ExchangeUnbound {
                                    exchange_name: self.exchange.name.clone(),
                                };

                                logerr!(b.queue.send(cmd).await);
                            }
                        }
                        Bindings::Topic(bs) => {
                            for b in bs {
                                let cmd = QueueCommand::ExchangeUnbound {
                                    exchange_name: self.exchange.name.clone(),
                                };

                                logerr!(b.queue.send(cmd).await);
                            }
                        }
                        Bindings::Fanout(bs) => {
                            for b in bs {
                                let cmd = QueueCommand::ExchangeUnbound {
                                    exchange_name: self.exchange.name.clone(),
                                };

                                logerr!(b.queue.send(cmd).await);
                            }
                        }
                        _ => (),
                    }

                    logerr!(result.send(Ok(())));

                    Ok(false)
                }
            }
        }
    }

    /// Route the message according to the exchange type and the bindings. If there is no queue to
    /// be send the message to, it gives back the messages in the Option.
    async fn route_message(&self, message: Message) -> Result<Option<Message>> {
        let mut sent = false;

        match &self.bindings {
            Bindings::Direct(bs) => {
                for binding in bs {
                    if binding.routing_key == message.routing_key {
                        let cmd = QueueCommand::PublishMessage(message.clone());
                        logerr!(binding.queue.send(cmd).await);
                        sent = true;
                    }
                }
            }
            Bindings::Fanout(bs) => {
                for binding in bs {
                    let cmd = QueueCommand::PublishMessage(message.clone());
                    logerr!(binding.queue.send(cmd).await);
                    sent = true;
                }
            }
            Bindings::Topic(bs) => {
                for binding in bs {
                    if match_routing_key(&binding.routing_key, &message.routing_key) {
                        let cmd = QueueCommand::PublishMessage(message.clone());
                        logerr!(binding.queue.send(cmd).await);
                        sent = true;
                    }
                }
            }
            _ => (),
        }

        if sent {
            Ok(None)
        } else {
            Ok(Some(message))
        }
    }
}

fn match_routing_key(binding_key: &str, message_routing_key: &str) -> bool {
    let mut bks: Vec<_> = binding_key.split('.').collect();
    let mut mks: Vec<_> = message_routing_key.split('.').collect();

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::MessageContent;
    use metalmq_codec::frame;

    #[test]
    fn test_match_routing_key() {
        assert!(match_routing_key("stocks.nwse.goog", "stocks.nwse.goog"));
    }

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
            bindings: Bindings::Direct(vec![]),
        };

        let msg = Message {
            source_connection: "conn-id".to_string(),
            channel: 2,
            content: MessageContent {
                body: b"Okay".to_vec(),
                ..Default::default()
            },
            exchange: "x-name".to_string(),
            routing_key: "".to_string(),
            mandatory: true,
            immediate: false,
        };
        let cmd = ExchangeCommand::Message {
            message: msg,
            outgoing: msg_tx,
        };
        let res = es.handle_command(cmd).await;
        assert!(res.is_ok());

        match recv_timeout(&mut msg_rx).await {
            Some(Frame::Frame(br)) => assert!(true),
            Some(Frame::Frames(fs)) => if let frame::AMQPFrame::Method(ch, cm, args) = fs.get(0).unwrap() {},
            None => assert!(false, "Basic.Return frame is expected"),
        }
    }
}
