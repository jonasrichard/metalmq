use crate::client;
use crate::client::conn::SendFrame;
use crate::exchange::Exchange;
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
        outgoing: mpsc::Sender<SendFrame>,
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

struct ExchangeState {
    exchange: super::Exchange,
    /// Queues bound by routing key
    queues: HashMap<String, QueueCommandSink>,
}

pub async fn start(
    exchange: Exchange,
    commands: &mut mpsc::Receiver<ExchangeCommand>,
    outgoing: mpsc::Sender<SendFrame>,
) {
    ExchangeState {
        exchange,
        queues: HashMap::new(),
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
                match self.choose_queue_by_routing_key(&message.routing_key) {
                    Some(queue) => {
                        // FIXME we can close a message as far as we don't use Vec but Bytes.
                        // Vec is cloned by cloning the underlying array, but Buffer is a bit
                        // more specialized, and it uses a reference counter pointer.
                        info!("Publish message {:?}", message);

                        if let Err(e) = send!(queue, QueueCommand::PublishMessage(message)) {
                            error!("Send error {:?}", e);
                        }
                    }
                    None => {
                        if message.mandatory {
                            // FIXME here self.outgoing is fatal, makes no sense, exchanges can be
                            // written by more than one producer!
                            message::send_basic_return(message, &outgoing).await?;
                        }
                    }
                }

                Ok(true)
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

                Ok(true)
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

                    result.send(Ok(()));

                    Ok(false)
                }
            }
        }
    }

    fn choose_queue_by_routing_key(&self, routing_key: &str) -> Option<&QueueCommandSink> {
        self.queues.get(routing_key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metalmq_codec::frame;

    async fn recv_timeout(rx: &mut mpsc::Receiver<SendFrame>) -> Option<Frame> {
        let sleep = tokio::time::sleep(tokio::time::Duration::from_secs(1));
        tokio::pin!(sleep);

        tokio::select! {
            frame = rx.recv() => {
                match frame {
                    Some(SendFrame::Async(f)) => return Some(f),
                    Some(SendFrame::Sync(f, _)) => return Some(f),
                    None => return None,
                }
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
                exchange_type: "direct".to_string(),
                durable: false,
                auto_delete: false,
                internal: false,
            },
            queues: HashMap::new(),
            outgoing: msg_tx,
        };

        let msg = Message {
            source_connection: "conn-id".to_string(),
            channel: 2,
            content: b"Hello".to_vec(),
            exchange: "x-name".to_string(),
            routing_key: "".to_string(),
            mandatory: true,
            immediate: false,
            content_type: None,
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
