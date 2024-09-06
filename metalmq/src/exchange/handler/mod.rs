#[cfg(test)]
mod tests;

use crate::{
    error::ChannelError,
    exchange::{binding::Bindings, Exchange, ExchangeType},
};
use crate::{
    logerr,
    message::Message,
    queue::handler::{QueueCommand, QueueCommandSink},
    send, Result,
};
use log::{debug, error, info, trace};
use metalmq_codec::frame::{self, FieldTable};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{mpsc, oneshot};

pub type ExchangeCommandSink = mpsc::Sender<ExchangeCommand>;

#[derive(Debug)]
pub struct QueueBindCmd {
    pub conn_id: String,
    pub channel: u16,
    pub queue_name: String,
    pub routing_key: String,
    pub args: Option<FieldTable>,
    pub sink: QueueCommandSink,
    pub result: oneshot::Sender<Result<bool>>,
}

#[derive(Debug)]
pub struct QueueUnbindCmd {
    pub channel: u16,
    pub queue_name: String,
    pub routing_key: String,
    pub result: oneshot::Sender<Result<bool>>,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum ExchangeCommand {
    Message {
        message: Message,
        /// If message is immediate or mandatory or if the channel is in confirm mode and the
        /// message routing failed, exchange needs to send back the message via this channel.
        returned: Option<oneshot::Sender<Option<Arc<Message>>>>,
    },
    QueueBind(QueueBindCmd),
    QueueUnbind(QueueUnbindCmd),
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
            ExchangeCommand::Message { message, returned } => {
                self.handle_message(message, returned).await?;

                Ok(true)
            }
            ExchangeCommand::QueueBind(cmd) => self.handle_queue_bind(cmd).await,
            ExchangeCommand::QueueUnbind(cmd) => {
                self.handle_queue_unbind(cmd).await?;

                Ok(true)
            }
            ExchangeCommand::Delete {
                channel,
                if_unused,
                result,
            } => Ok(self.handle_exchange_delete(channel, if_unused, result).await),
            ExchangeCommand::QueueDeleted { queue_name, result } => {
                self.bindings.remove_queue(&queue_name);

                debug!("{:?}", self.bindings);

                logerr!(result.send(()));

                Ok(true)
            }
        }
    }
    async fn handle_message(
        &self,
        message: Message,
        returned: Option<oneshot::Sender<Option<Arc<Message>>>>,
    ) -> Result<()> {
        if let Some(failed_message) = self.bindings.route_message(message).await? {
            // TODO handle immediate somewhere, too
            if failed_message.mandatory {
                returned.unwrap().send(Some(failed_message)).unwrap();
            } else if let Some(r) = returned {
                r.send(None).unwrap();
            }
        } else if let Some(r) = returned {
            r.send(None).unwrap();
        }

        Ok(())
    }

    async fn handle_queue_bind(&mut self, cmd: QueueBindCmd) -> Result<bool> {
        let (queue_info_tx, queue_info_rx) = oneshot::channel();

        match cmd.sink.send(QueueCommand::GetInfo { result: queue_info_tx }).await {
            Err(_) => cmd
                .result
                .send(ChannelError::NotFound.to_result(cmd.channel, frame::QUEUE_BIND, "Queue cannot be found"))
                .unwrap(),
            Ok(()) => match queue_info_rx.await {
                Err(_) => {
                    cmd.result
                        .send(ChannelError::NotFound.to_result(cmd.channel, frame::QUEUE_BIND, "Queue cannot be found"))
                        .unwrap();
                }
                Ok(queue_info) => {
                    info!("Got queue info {:?}", queue_info);

                    if queue_info.exclusive && cmd.conn_id != queue_info.declaring_connection {
                        cmd.result
                            .send(ChannelError::ResourceLocked.to_result(
                                cmd.channel,
                                frame::QUEUE_BIND,
                                "Cannot obtain exclusive access to queue, it is an exclusive queue declared by \
                            another connection",
                            ))
                            .unwrap();

                        return Ok(true);
                    }

                    // TODO refactor this to Bindings
                    let bind_result = match self.exchange.exchange_type {
                        ExchangeType::Direct => {
                            self.bindings
                                .add_direct_binding(cmd.routing_key, cmd.queue_name, cmd.sink.clone())
                        }
                        ExchangeType::Topic => {
                            self.bindings
                                .add_topic_binding(cmd.routing_key, cmd.queue_name, cmd.sink.clone())
                        }
                        ExchangeType::Fanout => self.bindings.add_fanout_binding(cmd.queue_name, cmd.sink.clone()),
                        ExchangeType::Headers => {
                            self.bindings
                                .add_headers_binding(cmd.queue_name, cmd.args, cmd.sink.clone())
                        }
                    };

                    if bind_result {
                        self.bound_queues.insert(queue_info.queue_name.clone(), queue_info);

                        let (tx, rx) = oneshot::channel();

                        logerr!(send!(
                            cmd.sink,
                            QueueCommand::ExchangeBound {
                                conn_id: cmd.conn_id.clone(),
                                channel: cmd.channel,
                                exchange_name: self.exchange.name.clone(),
                                result: tx,
                            }
                        ));

                        rx.await.unwrap()?;
                    }

                    logerr!(cmd.result.send(Ok(bind_result)));
                }
            },
        }

        Ok(true)
    }

    async fn handle_queue_unbind(&mut self, cmd: QueueUnbindCmd) -> Result<()> {
        info!(
            "Unbinding queue {} exchange {} routing key {}",
            cmd.queue_name, self.exchange.name, cmd.routing_key
        );

        // TODO refactor this to Bindings
        // TODO header binding?
        let sink = match self.exchange.exchange_type {
            ExchangeType::Direct => self.bindings.remove_direct_binding(&cmd.routing_key, &cmd.queue_name),
            ExchangeType::Topic => self.bindings.remove_topic_binding(&cmd.routing_key, &cmd.queue_name),
            ExchangeType::Fanout => self.bindings.remove_fanout_binding(&cmd.queue_name),
            _ => None,
        };

        match sink {
            Some(s) => {
                self.bound_queues.remove(&cmd.queue_name);

                let (tx, rx) = oneshot::channel();

                logerr!(send!(
                    s,
                    QueueCommand::ExchangeUnbound {
                        exchange_name: self.exchange.name.clone(),
                        result: tx,
                    }
                ));

                rx.await?.unwrap();

                self.bound_queues.remove(&cmd.queue_name);

                logerr!(cmd.result.send(Ok(true)));
            }
            None => {
                logerr!(cmd.result.send(Ok(false)));
            }
        }

        Ok(())
    }

    async fn handle_exchange_delete(
        &mut self,
        channel: u16,
        if_unused: bool,
        result: oneshot::Sender<Result<()>>,
    ) -> bool {
        if if_unused {
            if self.bindings.is_empty() {
                logerr!(result.send(Ok(())));

                false
            } else {
                let err =
                    ChannelError::PreconditionFailed.to_result(channel, frame::EXCHANGE_DELETE, "Exchange is in use");

                logerr!(result.send(err));

                true
            }
        } else {
            self.bindings
                .broadcast_exchange_unbound(&self.exchange.name)
                .await
                .unwrap();

            logerr!(result.send(Ok(())));

            false
        }
    }

    async fn handle_queue_deleted(&mut self) {}
}
