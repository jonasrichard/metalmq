use crate::client::{self, ChannelError};
use crate::exchange::handler::{self, ExchangeCommand, ExchangeCommandSink};
use crate::exchange::Exchange;
use crate::queue::handler::QueueCommandSink;
use crate::{logerr, Result};
use log::{debug, error, info, warn};
use metalmq_codec::codec::Frame;
use metalmq_codec::frame;
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

pub struct ExchangeState {
    pub exchange: Exchange,
    pub command_sink: ExchangeCommandSink,
}

#[derive(Debug)]
pub struct DeclareExchangeCommand {
    pub channel: u16,
    pub exchange: Exchange,
    pub passive: bool,
    pub outgoing: mpsc::Sender<Frame>,
}

#[derive(Debug)]
pub struct BindQueueCommand {
    pub conn_id: String,
    pub channel: u16,
    pub exchange_name: String,
    pub queue_name: String,
    pub routing_key: String,
    pub args: Option<frame::FieldTable>,
    pub queue_sink: QueueCommandSink,
}

#[derive(Debug)]
pub struct UnbindQueueCommand {
    pub conn_id: String,
    pub channel: u16,
    pub exchange_name: String,
    pub queue_name: String,
    pub routing_key: String,
}

#[derive(Debug)]
pub struct DeleteExchangeCommand {
    pub channel: u16,
    pub if_unused: bool,
    pub exchange_name: String,
}

#[derive(Debug)]
pub struct QueueDeletedEvent {
    pub queue_name: String,
}

#[derive(Debug)]
pub enum ExchangeManagerCommand {
    DeclareExchange(DeclareExchangeCommand, oneshot::Sender<Result<ExchangeCommandSink>>),
    BindQueue(BindQueueCommand, oneshot::Sender<Result<()>>),
    UnbindQueue(UnbindQueueCommand, oneshot::Sender<Result<()>>),
    DeleteExchange(DeleteExchangeCommand, oneshot::Sender<Result<()>>),
    GetExchanges(oneshot::Sender<Vec<Exchange>>),
    QueueDeleted(QueueDeletedEvent, oneshot::Sender<Result<()>>),
}

pub type ExchangeManagerSink = mpsc::Sender<ExchangeManagerCommand>;

/// Start exchange manager which manages the exchanges, exchange and queue bindings
/// via `ExchangeManagerCommand`.
pub fn start() -> ExchangeManagerSink {
    let (sink, stream) = mpsc::channel(1);

    tokio::spawn(async move {
        let mut manager = ExchangeManagerState {
            command_stream: stream,
            exchanges: HashMap::new(),
        };

        if let Err(e) = manager.command_loop().await {
            error!("Exchange manager exited {:?}", e);
        }
    });

    sink
}

pub async fn declare_exchange(mgr: &ExchangeManagerSink, cmd: DeclareExchangeCommand) -> Result<ExchangeCommandSink> {
    let (tx, rx) = oneshot::channel();

    mgr.send(ExchangeManagerCommand::DeclareExchange(cmd, tx)).await?;

    rx.await?
}

pub async fn bind_queue(mgr: &ExchangeManagerSink, cmd: BindQueueCommand) -> Result<()> {
    let (tx, rx) = oneshot::channel();

    mgr.send(ExchangeManagerCommand::BindQueue(cmd, tx)).await?;

    rx.await?
}

pub async fn unbind_queue(mgr: &ExchangeManagerSink, cmd: UnbindQueueCommand) -> Result<()> {
    let (tx, rx) = oneshot::channel();

    mgr.send(ExchangeManagerCommand::UnbindQueue(cmd, tx)).await?;

    rx.await?
}

pub async fn delete_exchange(mgr: &ExchangeManagerSink, cmd: DeleteExchangeCommand) -> Result<()> {
    let (tx, rx) = oneshot::channel();

    mgr.send(ExchangeManagerCommand::DeleteExchange(cmd, tx)).await?;

    rx.await?
}

pub async fn get_exchanges(mgr: &ExchangeManagerSink) -> Vec<Exchange> {
    let (tx, rx) = oneshot::channel();

    logerr!(mgr.send(ExchangeManagerCommand::GetExchanges(tx)).await);

    match rx.await {
        Ok(exchanges) => exchanges,
        Err(_) => vec![],
    }
}

pub async fn queue_deleted(mgr: &ExchangeManagerSink, evt: QueueDeletedEvent) -> Result<()> {
    let (tx, rx) = oneshot::channel();

    mgr.send(ExchangeManagerCommand::QueueDeleted(evt, tx)).await?;

    rx.await?
}

// -------------------------------------------------------------------

struct ExchangeManagerState {
    command_stream: mpsc::Receiver<ExchangeManagerCommand>,
    exchanges: HashMap<String, ExchangeState>,
}

impl ExchangeManagerState {
    async fn command_loop(&mut self) -> Result<()> {
        while let Some(command) = self.command_stream.recv().await {
            self.handle_command(command).await;
        }

        Ok(())
    }

    async fn handle_command(&mut self, command: ExchangeManagerCommand) {
        use ExchangeManagerCommand::*;

        match command {
            DeclareExchange(command, tx) => {
                logerr!(tx.send(self.handle_declare_exchange(command)));
            }
            BindQueue(command, tx) => {
                logerr!(tx.send(self.handle_bind_queue(command).await));
            }
            UnbindQueue(command, tx) => {
                logerr!(tx.send(self.handle_unbind_queue(command).await));
            }
            DeleteExchange(command, tx) => {
                logerr!(tx.send(self.handle_delete_exchange(command).await));
            }
            GetExchanges(tx) => {
                logerr!(tx.send(self.handle_exchange_list()));
            }
            QueueDeleted(evt, tx) => {
                logerr!(tx.send(self.handle_queue_deleted(evt).await));
            }
        }
    }

    /// Declare an exchange if it doesn't exist. If passive is true, the declaration is rather a
    /// check if the exchange exists.
    fn handle_declare_exchange(&mut self, command: DeclareExchangeCommand) -> Result<ExchangeCommandSink> {
        debug!("Declare exchange {:?}", command.exchange);

        validate_exchange_name(command.channel, &command.exchange.name)?;

        match self.exchanges.get(&command.exchange.name) {
            None if command.passive => client::channel_error(
                command.channel,
                frame::EXCHANGE_DECLARE,
                ChannelError::NotFound,
                &format!("NOT_FOUND - no exchange '{}' in vhost '/'", command.exchange.name),
            ),
            Some(exchg) => {
                if exchg.exchange != command.exchange {
                    client::channel_error(
                        command.channel,
                        frame::EXCHANGE_DECLARE,
                        ChannelError::PreconditionFailed,
                        "PRECONDITION_FAILED - Exchange exists but properties are different",
                    )
                } else {
                    Ok(exchg.command_sink.clone())
                }
            }
            None => {
                let (command_sink, mut command_stream) = mpsc::channel(1);
                let exchange_clone = command.exchange.clone();

                tokio::spawn(async move {
                    handler::start(exchange_clone, &mut command_stream).await;
                });

                let exchange_name = command.exchange.name.clone();
                let exchange_state = ExchangeState {
                    exchange: command.exchange,
                    command_sink: command_sink.clone(),
                };

                self.exchanges.insert(exchange_name, exchange_state);

                Ok(command_sink)
            }
        }
    }

    async fn handle_bind_queue(&self, command: BindQueueCommand) -> Result<()> {
        match self.exchanges.get(&command.exchange_name) {
            Some(exchange_state) => {
                let (tx, rx) = oneshot::channel();

                let cmd = ExchangeCommand::QueueBind {
                    conn_id: command.conn_id,
                    channel: command.channel,
                    queue_name: command.queue_name.to_string(),
                    routing_key: command.routing_key.to_string(),
                    args: command.args,
                    sink: command.queue_sink,
                    result: tx,
                };

                exchange_state.command_sink.send(cmd).await?;

                rx.await?.map(|_| ())
            }
            None => {
                warn!("Exchange not found {}", command.exchange_name);

                client::channel_error(command.channel, frame::QUEUE_BIND, ChannelError::NotFound, "Not found")
            }
        }
    }

    async fn handle_unbind_queue(&self, command: UnbindQueueCommand) -> Result<()> {
        info!("Unbind queue {:?}", command);

        match self.exchanges.get(&command.exchange_name) {
            Some(exchange_state) => {
                let (tx, rx) = oneshot::channel();

                let cmd = ExchangeCommand::QueueUnbind {
                    channel: command.channel,
                    queue_name: command.queue_name.to_string(),
                    routing_key: command.routing_key.to_string(),
                    result: tx,
                };

                exchange_state.command_sink.send(cmd).await?;
                rx.await?.map(|_| ())
            }
            None => client::channel_error(
                command.channel,
                frame::QUEUE_UNBIND,
                ChannelError::NotFound,
                "Exchange not found",
            ),
        }

        // TODO we need to have a checked which reaps orphaned exchanges (no queue, no connection
        // and channel belonging to them)
    }

    async fn handle_delete_exchange(&mut self, command: DeleteExchangeCommand) -> Result<()> {
        if let Some(exchange) = self.exchanges.get(&command.exchange_name) {
            let (tx, rx) = oneshot::channel();

            let cmd = ExchangeCommand::Delete {
                channel: command.channel,
                if_unused: command.if_unused,
                result: tx,
            };
            exchange.command_sink.send(cmd).await.unwrap();

            let delete_result = rx.await?;

            if delete_result.is_ok() {
                self.exchanges.remove(&command.exchange_name);
            }

            delete_result
        } else {
            client::channel_error(
                command.channel,
                frame::EXCHANGE_DELETE,
                ChannelError::NotFound,
                "Exchange not found",
            )
        }
    }

    fn handle_exchange_list(&self) -> Vec<Exchange> {
        self.exchanges.values().map(|e| e.exchange.clone()).collect()
    }

    async fn handle_queue_deleted(&mut self, evt: QueueDeletedEvent) -> Result<()> {
        for exchange in self.exchanges.values() {
            let (tx, rx) = oneshot::channel();

            let cmd = ExchangeCommand::QueueDeleted {
                queue_name: evt.queue_name.clone(),
                result: tx,
            };
            exchange.command_sink.send(cmd).await.unwrap();

            rx.await?
        }

        Ok(())
    }
}

fn validate_exchange_name(channel: u16, exchange_name: &str) -> Result<()> {
    let spec = String::from("_-:.");

    for c in exchange_name.chars() {
        if !c.is_alphanumeric() && spec.find(c).is_none() {
            return client::channel_error(
                channel,
                frame::EXCHANGE_DECLARE,
                ChannelError::PreconditionFailed,
                "PRECONDITION_FAILED - Exchange contains invalid character",
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::ExchangeType;
    use crate::{ErrorScope, RuntimeError};

    #[tokio::test]
    async fn passive_declare_exchange_does_not_exists_channel_error() {
        let (_cmd_tx, cmd_rx) = mpsc::channel(1);
        let mut manager = ExchangeManagerState {
            command_stream: cmd_rx,
            exchanges: HashMap::new(),
        };
        let exchange = Exchange {
            name: "passive-exchg".into(),
            ..Default::default()
        };
        let (tx, _rx) = mpsc::channel(1);
        let cmd = DeclareExchangeCommand {
            channel: 1,
            exchange,
            passive: true,
            outgoing: tx,
        };
        let result = manager.handle_declare_exchange(cmd);

        assert!(result.is_err());
        let err = result.unwrap_err().downcast::<RuntimeError>().unwrap();
        assert_eq!(err.code, ChannelError::NotFound as u16);
    }

    #[tokio::test]
    async fn declare_exchange_exists_fields_different_error() {
        let (_cmd_tx, cmd_rx) = mpsc::channel(1);
        let mut manager = ExchangeManagerState {
            command_stream: cmd_rx,
            exchanges: HashMap::new(),
        };
        let (tx, _) = mpsc::channel(1);
        manager.exchanges.insert(
            "temp-changes".into(),
            ExchangeState {
                exchange: Exchange {
                    name: "temp-changes".to_string(),
                    exchange_type: ExchangeType::Topic,
                    ..Default::default()
                },
                command_sink: tx,
            },
        );

        let exchange = Exchange {
            name: "temp-changes".into(),
            exchange_type: ExchangeType::Direct,
            ..Default::default()
        };

        let (tx, _rx) = mpsc::channel(1);
        let cmd = DeclareExchangeCommand {
            channel: 1,
            exchange,
            passive: false,
            outgoing: tx,
        };
        let result = manager.handle_declare_exchange(cmd);

        assert!(result.is_err());

        let err = result.unwrap_err().downcast::<RuntimeError>().unwrap();
        assert_eq!(err.scope, ErrorScope::Channel);
        assert_eq!(err.code, ChannelError::PreconditionFailed as u16);
    }

    #[tokio::test]
    async fn declare_exchange_does_not_exist_created() {
        let (_cmd_tx, cmd_rx) = mpsc::channel(1);
        let mut manager = ExchangeManagerState {
            command_stream: cmd_rx,
            exchanges: HashMap::new(),
        };
        let exchange = Exchange {
            name: "transactions".into(),
            exchange_type: ExchangeType::Direct,
            durable: true,
            auto_delete: true,
            internal: true,
        };
        let (tx, _rx) = mpsc::channel(1);
        let cmd = DeclareExchangeCommand {
            channel: 1,
            exchange,
            passive: false,
            outgoing: tx,
        };
        let result = manager.handle_declare_exchange(cmd);

        assert!(result.is_ok());

        let state = manager.exchanges.get("transactions").unwrap();
        assert_eq!(state.exchange.exchange_type, ExchangeType::Direct);
        assert_eq!(state.exchange.durable, true);
        assert_eq!(state.exchange.auto_delete, true);
        assert_eq!(state.exchange.internal, true);
    }
}
