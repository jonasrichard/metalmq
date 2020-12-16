//! Exchanges Handle incoming messages and forward them to other exhanges
//! or queues. Each exchange is a lightweight process and handle messages
//! through a channel. When a client is publishing to an exchange it should
//! clone the exchange channel, so the messages will be handled serially.
use crate::{conn_state, message, ErrorScope, Result, RuntimeError};
use log::debug;
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

pub(crate) struct Exchanges {
    exchanges: HashMap<String, Exchange>,
}

#[derive(Debug)]
pub(crate) struct IncomingMessage {
    message: message::Message,
    ready: oneshot::Sender<()>,
}

#[derive(Debug)]
pub(crate) struct Exchange {
    name: String,
    exchange_type: String,
    durable: bool,
    auto_delete: bool,
    internal: bool,
    input: mpsc::Sender<IncomingMessage>,
}

pub(crate) fn start() -> Exchanges {
    Exchanges {
        exchanges: HashMap::new(), // TODO add default exchanges from a config or db
    }
}

pub(crate) async fn declare(
    exchanges: &mut Exchanges,
    name: String,
    exchange_type: String,
    durable: bool,
    auto_delete: bool,
    internal: bool,
    passive: bool,
) -> Result<()> {
    match exchanges.exchanges.get(&name) {
        None => {
            if passive {
                Err(Box::new(RuntimeError {
                    scope: ErrorScope::Channel,
                    code: 404,
                    text: "Exchange not found".into(),
                    ..Default::default()
                }))
            } else {
                create_exchange(
                    &mut exchanges.exchanges,
                    name,
                    exchange_type,
                    durable,
                    auto_delete,
                    internal,
                )
                .await?;

                Ok(())
            }
        }
        Some(ex) => {
            if passive {
                Ok(())
            } else {
                if ex.exchange_type == exchange_type
                    || ex.durable != durable
                    || ex.auto_delete != auto_delete
                    || ex.internal != internal
                {
                    Err(Box::new(RuntimeError {
                        scope: ErrorScope::Channel,
                        code: conn_state::PRECONDITION_FAILED,
                        text: "Exchange exists but properties are different".into(),
                        ..Default::default()
                    }))
                } else {
                    Ok(())
                }
            }
        }
    }
}

async fn create_exchange(
    exchanges: &mut HashMap<String, Exchange>,
    name: String,
    exchange_type: String,
    durable: bool,
    auto_delete: bool,
    internal: bool,
) -> Result<()> {
    let (sender, mut receiver) = mpsc::channel(1);
    let exchange = Exchange {
        name: name.clone(),
        exchange_type: exchange_type,
        durable: durable,
        auto_delete: auto_delete,
        internal: internal,
        input: sender,
    };

    debug!("New exchange {:?}", exchange);
    exchanges.insert(name, exchange);

    tokio::spawn(async move {
        exchange_loop(&mut receiver);
    });

    Ok(())
}

async fn exchange_loop(messages: &mut mpsc::Receiver<IncomingMessage>) {
    while let Some(message) = messages.recv().await {
        debug!("Exchange: {:?}", message);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn passive_declare_exchange_does_not_exists_channel_error() {
        let mut exchanges = HashMap::new();

        let result = declare(
            &mut exchanges,
            "new exchange".into(),
            false,
            false,
            false,
            true,
        )
        .await;

        assert!(result.is_err());
        let err = result.unwrap_err().downcast::<RuntimeError>().unwrap();
        assert_eq!(err.code, 404);
    }

    #[tokio::test]
    async fn declare_exchange_exists_fields_different_error() {
        let mut exchanges = HashMap::new();
        let (sender, _receiver) = mpsc::channel(1);
        let exchange_name = "orders".to_string();

        exchanges.insert(
            exchange_name.clone(),
            Exchange {
                name: exchange_name.clone(),
                durable: false,
                auto_delete: true,
                internal: false,
                input: sender,
            },
        );

        let result = declare(&mut exchanges, exchange_name, true, true, false, false).await;

        assert!(result.is_err());

        let err = result.unwrap_err().downcast::<RuntimeError>().unwrap();
        assert_eq!(err.scope, ErrorScope::Channel);
        assert_eq!(err.code, conn_state::PRECONDITION_FAILED);
    }

    #[tokio::test]
    async fn declare_exchange_does_not_exist_created() {
        let mut exchanges = HashMap::new();
        let exchange_name = "orders".to_string();

        let result = declare(
            &mut exchanges,
            exchange_name.clone(),
            true,
            true,
            false,
            false,
        )
        .await;

        assert!(result.is_ok());

        let ex = exchanges.get(&exchange_name).unwrap();
        assert_eq!(ex.name, exchange_name);
        assert_eq!(ex.durable, true);
        assert_eq!(ex.auto_delete, true);
        assert_eq!(ex.internal, false);
    }
}
