use crate::client::{channel_error, connection_error, ChannelError, ConnectionError};
use crate::exchange::handler::{self, ExchangeCommand, ExchangeCommandSink};
use crate::exchange::Exchange;
use crate::queue::handler::QueueCommandSink;
use crate::Result;
use log::{debug, error};
use metalmq_codec::frame;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

pub(crate) struct ExchangeManager {
    exchanges: Arc<Mutex<HashMap<String, ExchangeState>>>,
}

struct ExchangeState {
    exchange: Exchange,
    command_sink: ExchangeCommandSink,
}

pub(crate) fn start() -> ExchangeManager {
    ExchangeManager {
        exchanges: Arc::new(Mutex::new(HashMap::new())), // TODO add default exchanges from a config or db
    }
}

/// Managing exchanges in the server. Connections can create, delete exchanges, bind them to
/// queues and so on.
impl ExchangeManager {
    /// Make it sure that exchange exists, so if it hasn't existed it creates that. If `passive` is
    /// true it doesn't create channel if it doesn't exist. So with passive declare one can check
    /// if channel exists or doesn't. Otherwise if channel already exists all the parameters need
    /// to be the same as in the exchange given as a parameter.
    pub(crate) async fn declare(
        &mut self,
        exchange: Exchange,
        passive: bool,
        _conn: &str,
    ) -> Result<ExchangeCommandSink> {
        let mut ex = self.exchanges.lock().await;

        debug!("Declare exchange: {:?}", exchange);

        validate_exchange_name(&exchange.name)?;
        validate_exchange_type(&exchange.exchange_type)?;

        match ex.get(&exchange.name) {
            None => {
                if passive {
                    return channel_error(
                        0,
                        frame::EXCHANGE_DECLARE,
                        ChannelError::NotFound,
                        &format!("NOT_FOUND - no exchange '{}' in vhost '/'", exchange.name),
                    );
                }
            }
            Some(current) => {
                debug!("Current instance {:?}", current.exchange);

                // TODO: the server must ignore the auto delete field if the exchange already
                // exists
                if current.exchange != exchange {
                    error!(
                        "Current exchange: {:?} to be declared. {:?}",
                        current.exchange, exchange
                    );

                    return channel_error(
                        0,
                        frame::EXCHANGE_DECLARE,
                        ChannelError::PreconditionFailed,
                        "PRECONDITION_FAILED - Exchange exists but properties are different",
                    );
                }
            }
        }

        let (command_sink, mut command_stream) = mpsc::channel(1);

        tokio::spawn(async move {
            handler::exchange_loop(&mut command_stream).await.unwrap();
        });

        let exchange_name = exchange.name.clone();

        let exchange_state = ExchangeState {
            exchange,
            command_sink: command_sink.clone(),
        };

        ex.insert(exchange_name, exchange_state);

        Ok(command_sink)
    }

    pub(crate) async fn bind_queue(&mut self, exchange_name: String, queue_channel: QueueCommandSink) -> Result<()> {
        let ex = self.exchanges.lock().await;

        match ex.get(&exchange_name) {
            Some(exchange_state) => {
                // TODO we need to have a oneshot channel here to wait for the result
                exchange_state
                    .command_sink
                    .send(ExchangeCommand::QueueBind { sink: queue_channel })
                    .await
                    .unwrap();

                Ok(())
            }
            None => channel_error(0, frame::QUEUE_BIND, ChannelError::NotFound, "Not found"),
        }
    }

    pub(crate) async fn unbind_queue(&mut self, exchange_name: &str, queue_name: &str) -> Result<()> {
        // TODO unbound queue
        // TODO we need to have a checked which reaps orphaned exchanges (no queue, no connection
        // and channel belonging to them)
        Ok(())
    }

    pub(crate) async fn delete_exchange(&mut self, exchange_name: &str) -> Result<()> {
        Ok(())
    }

    pub(crate) async fn exchange_list(&self) -> Vec<Exchange> {
        let ex = self.exchanges.lock().await;

        ex.values().map(|e| e.exchange.clone()).collect()
    }
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
        assert_eq!(err.code, 404);
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
        assert_eq!(err.code, ChannelError::PreconditionFailed);
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
