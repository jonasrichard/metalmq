use super::binding::Bindings;
use crate::client::{self, channel_error, ChannelError};
use crate::exchange::{Exchange, ExchangeType};
use crate::message::{self, Message};
use crate::queue::handler::{QueueCommand, QueueCommandSink};
use crate::{logerr, send, Result};
use log::{debug, error, info, trace};
use metalmq_codec::codec::Frame;
use metalmq_codec::frame::{self, FieldTable};
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

pub type ExchangeCommandSink = mpsc::Sender<ExchangeCommand>;

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum ExchangeCommand {
    Message {
        message: Message,
        outgoing: mpsc::Sender<Frame>,
    },
    QueueBind {
        conn_id: String,
        channel: u16,
        queue_name: String,
        routing_key: String,
        args: Option<FieldTable>,
        sink: QueueCommandSink,
        result: oneshot::Sender<Result<bool>>,
    },
    QueueUnbind {
        channel: u16,
        queue_name: String,
        routing_key: String,
        result: oneshot::Sender<Result<bool>>,
    },
    Delete {
        channel: u16,
        if_unused: bool,
        result: oneshot::Sender<Result<()>>,
    },
    QueueDeleted {
        queue_name: String,
        result: oneshot::Sender<()>,
    },
}

#[derive(Debug)]
pub struct QueueInfo {
    pub queue_name: String,
    pub declaring_connection: String,
    pub exclusive: bool,
    pub durable: bool,
    pub auto_delete: bool,
}

struct ExchangeState {
    exchange: super::Exchange,
    /// Queue bindings
    bindings: Bindings,
    /// Bound queues
    bound_queues: HashMap<String, QueueInfo>,
}

pub async fn start(exchange: Exchange, commands: &mut mpsc::Receiver<ExchangeCommand>) {
    let et = exchange.exchange_type.clone();

    ExchangeState {
        exchange,
        bindings: Bindings::new(et),
        bound_queues: HashMap::new(),
    }
    .exchange_loop(commands)
    .await
    .unwrap();
}

impl ExchangeState {
    pub async fn exchange_loop(&mut self, commands: &mut mpsc::Receiver<ExchangeCommand>) -> Result<()> {
        while let Some(command) = commands.recv().await {
            trace!("Command {command:?}");

            if !self.handle_command(command).await.unwrap() {
                break;
            }
        }

        Ok(())
    }

    pub async fn handle_command(&mut self, command: ExchangeCommand) -> Result<bool> {
        match command {
            ExchangeCommand::Message { message, outgoing } => {
                if let Some(failed_message) = self.bindings.route_message(message).await? {
                    if failed_message.mandatory {
                        message::send_basic_return(failed_message, &outgoing).await?;
                    }
                }

                Ok(true)
            }
            ExchangeCommand::QueueBind {
                conn_id,
                channel,
                queue_name,
                routing_key,
                args,
                sink,
                result,
            } => {
                let (queue_info_tx, queue_info_rx) = oneshot::channel();

                sink.send(QueueCommand::GetInfo { result: queue_info_tx });

                let queue_info = queue_info_rx.await.unwrap();

                if conn_id != queue_info.declaring_connection {
                    result.send(channel_error(
                        channel,
                        frame::QUEUE_BIND,
                        ChannelError::AccessRefused,
                        "Exclusive queue belongs to another connections",
                    ));

                    return Ok(true);
                }

                // TODO refactor this to Bindings
                let bind_result = match self.exchange.exchange_type {
                    ExchangeType::Direct => self.bindings.add_direct_binding(routing_key, queue_name, sink.clone()),
                    ExchangeType::Topic => self.bindings.add_topic_binding(routing_key, queue_name, sink.clone()),
                    ExchangeType::Fanout => self.bindings.add_fanout_binding(queue_name, sink.clone()),
                    ExchangeType::Headers => self.bindings.add_headers_binding(queue_name, args, sink.clone()),
                };

                if bind_result {
                    self.bound_queues.insert(queue_info.queue_name.clone(), queue_info);

                    let (tx, rx) = oneshot::channel();

                    logerr!(send!(
                        sink,
                        QueueCommand::ExchangeBound {
                            conn_id: conn_id.clone(),
                            channel,
                            exchange_name: self.exchange.name.clone(),
                            result: tx,
                        }
                    ));

                    rx.await.unwrap();
                }

                logerr!(result.send(Ok(bind_result)));

                Ok(true)
            }
            ExchangeCommand::QueueUnbind {
                channel,
                queue_name,
                routing_key,
                result,
            } => {
                info!(
                    "Unbinding queue {} exchange {} routing key {}",
                    queue_name, self.exchange.name, routing_key
                );

                // TODO refactor this to Bindings
                let sink = match self.exchange.exchange_type {
                    ExchangeType::Direct => self.bindings.remove_direct_binding(&routing_key, &queue_name),
                    ExchangeType::Topic => self.bindings.remove_topic_binding(&routing_key, &queue_name),
                    ExchangeType::Fanout => self.bindings.remove_fanout_binding(&queue_name),
                    _ => None,
                };

                match sink {
                    Some(s) => {
                        let queue_info = self.bound_queues.get(&queue_name).unwrap();
                        let (tx, rx) = oneshot::channel();

                        logerr!(send!(
                            s,
                            QueueCommand::ExchangeUnbound {
                                exchange_name: self.exchange.name.clone(),
                                result: tx,
                            }
                        ));

                        rx.await?;

                        logerr!(result.send(Ok(true)));
                    }
                    None => {
                        logerr!(result.send(Ok(false)));
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
                        logerr!(result.send(Ok(())));

                        Ok(false)
                    } else {
                        let err = client::channel_error(
                            channel,
                            frame::EXCHANGE_DELETE,
                            client::ChannelError::PreconditionFailed,
                            "Exchange is in use",
                        );

                        logerr!(result.send(err));

                        Ok(true)
                    }
                } else {
                    self.bindings
                        .broadcast_exchange_unbound(self.exchange.name.clone())
                        .await;

                    logerr!(result.send(Ok(())));

                    Ok(false)
                }
            }
            ExchangeCommand::QueueDeleted { queue_name, result } => {
                self.bindings.remove_queue(&queue_name);

                debug!("{:?}", self.bindings);

                logerr!(result.send(()));

                Ok(true)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::MessageContent;
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
            bindings: Bindings::Direct(vec![]),
            bound_queues: HashMap::new(),
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
            Some(Frame::Frame(_br)) => assert!(true),
            Some(Frame::Frames(fs)) => if let frame::AMQPFrame::Method(_ch, _cm, _args) = fs.get(0).unwrap() {},
            None => assert!(false, "Basic.Return frame is expected"),
        }
    }
}
