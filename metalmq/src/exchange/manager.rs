use crate::client::{channel_error, connection_error, ChannelError, ConnectionError};
use crate::exchange::handler::{self, ExchangeCommand, ExchangeCommandSink};
use crate::exchange::Exchange;
use crate::queue::handler::QueueCommandSink;
use crate::{logerr, Result};
use log::{debug, error, trace};
use metalmq_codec::frame;
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

struct ExchangeState {
    exchange: Exchange,
    command_sink: ExchangeCommandSink,
}

#[derive(Debug)]
pub(crate) enum ExchangeManagerCommand {
    DeclareExchange {
        exchange: Exchange,
        passive: bool,
        result: oneshot::Sender<Result<ExchangeCommandSink>>,
    },
    BindQueue {
        exchange_name: String,
        queue_name: String,
        routing_key: String,
        queue_sink: QueueCommandSink,
        result: oneshot::Sender<Result<()>>,
    },
    UnbindQueue {
        exchange_name: String,
        queue_name: String,
        routing_key: String,
        result: oneshot::Sender<Result<()>>,
    },
    DeleteExchange {
        exchange_name: String,
        result: oneshot::Sender<Result<()>>,
    },
    GetExchanges {
        result: oneshot::Sender<Vec<Exchange>>,
    },
}

pub(crate) type ExchangeManagerSink = mpsc::Sender<ExchangeManagerCommand>;

/// Start exchange manager which manages the exchanges, exchange and queue bindings
/// via `ExchangeManagerCommand`.
pub(crate) fn start() -> ExchangeManagerSink {
    let (sink, mut stream) = mpsc::channel(1);

    tokio::spawn(async move {
        if let Err(e) = command_loop(&mut stream).await {
            error!("Exchange manager exited {:?}", e);
        }
    });

    sink
}

pub(crate) async fn declare_exchange(
    mgr: &ExchangeManagerSink,
    exchange: Exchange,
    passive: bool,
) -> Result<ExchangeCommandSink> {
    let (tx, rx) = oneshot::channel();

    mgr.send(ExchangeManagerCommand::DeclareExchange {
        exchange,
        passive,
        result: tx,
    })
    .await?;

    rx.await?
}

pub(crate) async fn bind_queue(
    mgr: &ExchangeManagerSink,
    exchange_name: &str,
    queue_name: &str,
    routing_key: &str,
    queue_sink: QueueCommandSink,
) -> Result<()> {
    let (tx, rx) = oneshot::channel();

    mgr.send(ExchangeManagerCommand::BindQueue {
        exchange_name: exchange_name.to_string(),
        queue_name: queue_name.to_string(),
        routing_key: routing_key.to_string(),
        queue_sink,
        result: tx,
    })
    .await?;

    rx.await?
}

pub(crate) async fn delete_exchange(mgr: &ExchangeManagerSink, exchange_name: &str) -> Result<()> {
    let (tx, rx) = oneshot::channel();

    mgr.send(ExchangeManagerCommand::DeleteExchange {
        exchange_name: exchange_name.to_string(),
        result: tx,
    })
    .await?;

    rx.await?
}

pub(crate) async fn get_exchanges(mgr: &ExchangeManagerSink) -> Vec<Exchange> {
    let (tx, rx) = oneshot::channel();

    logerr!(mgr.send(ExchangeManagerCommand::GetExchanges { result: tx }).await);

    match rx.await {
        Ok(exchanges) => exchanges,
        Err(_) => vec![],
    }
}

async fn command_loop(stream: &mut mpsc::Receiver<ExchangeManagerCommand>) -> Result<()> {
    let mut exchanges = HashMap::<String, ExchangeState>::new();

    while let Some(command) = stream.recv().await {
        trace!("Command {:?}", command);

        handle_command(&mut exchanges, command).await;
    }

    Ok(())
}

async fn handle_command(mut exchanges: &mut HashMap<String, ExchangeState>, command: ExchangeManagerCommand) {
    use ExchangeManagerCommand::*;

    match command {
        DeclareExchange {
            exchange,
            passive,
            result,
        } => {
            logerr!(result.send(handle_declare_exchange(&mut exchanges, exchange, passive)));
        }
        BindQueue {
            exchange_name,
            queue_name,
            routing_key,
            queue_sink,
            result,
        } => {
            logerr!(
                result.send(handle_bind_queue(&exchanges, &exchange_name, &queue_name, &routing_key, queue_sink).await)
            );
        }
        UnbindQueue {
            exchange_name,
            queue_name,
            routing_key,
            result,
        } => {
            logerr!(result.send(handle_unbind_queue(&exchanges, &exchange_name, &queue_name, &routing_key).await));
        }
        DeleteExchange { exchange_name, result } => {
            logerr!(result.send(handle_delete_exchange(&mut exchanges, &exchange_name).await));
        }
        GetExchanges { result } => {
            logerr!(result.send(handle_exchange_list(&exchanges)));
        }
    }
}

/// Declare an exchange if it doesn't exist. If passive is true, the declaration is rather a
/// check if the exchange exists.
fn handle_declare_exchange(
    exchanges: &mut HashMap<String, ExchangeState>,
    exchange: Exchange,
    passive: bool,
) -> Result<ExchangeCommandSink> {
    debug!("Declare exchange {:?}", exchange);

    validate_exchange_name(&exchange.name)?;
    validate_exchange_type(&exchange.exchange_type)?;

    match exchanges.get(&exchange.name) {
        None if passive => channel_error(
            0,
            frame::EXCHANGE_DECLARE,
            ChannelError::NotFound,
            &format!("NOT_FOUND - no exchange '{}' in vhost '/'", exchange.name),
        ),
        Some(exchg) => {
            if exchg.exchange != exchange {
                channel_error(
                    0,
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
            let exchange_clone = exchange.clone();

            tokio::spawn(async move {
                handler::exchange_loop(exchange_clone, &mut command_stream)
                    .await
                    .unwrap();
            });

            let exchange_name = exchange.name.clone();

            let exchange_state = ExchangeState {
                exchange,
                command_sink: command_sink.clone(),
            };

            exchanges.insert(exchange_name, exchange_state);

            Ok(command_sink)
        }
    }
}

async fn handle_bind_queue(
    exchanges: &HashMap<String, ExchangeState>,
    exchange_name: &str,
    queue_name: &str,
    routing_key: &str,
    queue_channel: QueueCommandSink,
) -> Result<()> {
    match exchanges.get(exchange_name) {
        Some(exchange_state) => {
            let (tx, rx) = oneshot::channel();

            let cmd = ExchangeCommand::QueueBind {
                queue_name: queue_name.to_string(),
                routing_key: routing_key.to_string(),
                sink: queue_channel,
                result: tx,
            };

            exchange_state.command_sink.send(cmd).await?;
            rx.await?;

            Ok(())
        }
        None => channel_error(0, frame::QUEUE_BIND, ChannelError::NotFound, "Not found"),
    }
}

async fn handle_unbind_queue(
    exchanges: &HashMap<String, ExchangeState>,
    exchange_name: &str,
    queue_name: &str,
    routing_key: &str,
) -> Result<()> {
    match exchanges.get(exchange_name) {
        Some(exchange_state) => {
            let (tx, rx) = oneshot::channel();

            let cmd = ExchangeCommand::QueueUnbind {
                queue_name: queue_name.to_string(),
                routing_key: routing_key.to_string(),
                result: tx,
            };

            exchange_state.command_sink.send(cmd).await?;
            rx.await?;

            Ok(())
        }
        None => channel_error(0, frame::QUEUE_UNBIND, ChannelError::NotFound, "Exchange not found"),
    }

    // TODO we need to have a checked which reaps orphaned exchanges (no queue, no connection
    // and channel belonging to them)
}

async fn handle_delete_exchange(exchanges: &mut HashMap<String, ExchangeState>, exchange_name: &str) -> Result<()> {
    Ok(())
}

fn handle_exchange_list(exchanges: &HashMap<String, ExchangeState>) -> Vec<Exchange> {
    exchanges.values().map(|e| e.exchange.clone()).collect()
}

fn validate_exchange_name(exchange_name: &str) -> Result<()> {
    let spec = String::from("_-:.");

    for c in exchange_name.chars() {
        if !c.is_alphanumeric() && spec.find(c).is_none() {
            return channel_error(
                0,
                frame::EXCHANGE_DECLARE,
                ChannelError::PreconditionFailed,
                "PRECONDITION_FAILED - Exchange contains invalid character",
            );
        }
    }

    Ok(())
}

fn validate_exchange_type(exchange_type: &str) -> Result<()> {
    let allowed_type = ["direct", "topic", "fanout"];

    if !allowed_type.contains(&exchange_type) {
        return connection_error(
            frame::EXCHANGE_DECLARE,
            ConnectionError::CommandInvalid,
            "PRECONDITION_FAILED - Exchange type is invalid",
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ErrorScope, RuntimeError};
    use metalmq_codec::frame::{ExchangeDeclareArgs, ExchangeDeclareFlags};

    #[tokio::test]
    async fn passive_declare_exchange_does_not_exists_channel_error() {
        let mut exchanges = start();
        let mut args = ExchangeDeclareArgs::default();

        args.exchange_name = "new exchange".to_string();
        args.flags |= ExchangeDeclareFlags::PASSIVE;

        let result = exchanges.declare(args.into(), true, "").await;

        assert!(result.is_err());
        let err = result.unwrap_err().downcast::<RuntimeError>().unwrap();
        assert_eq!(err.code, ChannelError::PreconditionFailed as u16);
    }

    #[tokio::test]
    async fn declare_exchange_exists_fields_different_error() {
        let exchange_name = "orders".to_string();
        let exchange_type = "fanout".to_string();

        let mut exchanges = start();

        let mut args = ExchangeDeclareArgs::default();
        args.exchange_name = exchange_name.clone();
        args.exchange_type = exchange_type.clone();

        let _ = exchanges.declare(args.into(), false, "").await;

        let mut args2 = ExchangeDeclareArgs::default();
        args2.exchange_name = exchange_name.clone();
        args2.exchange_type = "topic".to_string();

        let result = exchanges.declare(args2.into(), false, "").await;

        assert!(result.is_err());

        let err = result.unwrap_err().downcast::<RuntimeError>().unwrap();
        assert_eq!(err.scope, ErrorScope::Channel);
        assert_eq!(err.code, ChannelError::PreconditionFailed as u16);
    }

    #[tokio::test]
    async fn declare_exchange_does_not_exist_created() {
        let mut exchanges = start();
        let exchange_name = "orders".to_string();

        let mut args = ExchangeDeclareArgs::default();
        args.exchange_name = exchange_name.clone();
        args.flags |= ExchangeDeclareFlags::DURABLE;
        args.flags |= ExchangeDeclareFlags::AUTO_DELETE;

        let result = exchanges.declare(args.into(), false, "").await;

        assert!(result.is_ok());

        let ex = exchanges.exchanges.lock().await;
        let state = ex.get(&exchange_name).unwrap();
        assert_eq!(state.exchange.name, exchange_name);
        assert_eq!(state.exchange.durable, true);
        assert_eq!(state.exchange.auto_delete, true);
        assert_eq!(state.exchange.internal, false);
    }
}
