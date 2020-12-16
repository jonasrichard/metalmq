//! Exchanges Handle incoming messages and forward them to other exhanges
//! or queues. Each exchange is a lightweight process and handle messages
//! through a channel. When a client is publishing to an exchange it should
//! clone the exchange channel, so the messages will be handled serially.
use crate::{conn_state, message, ErrorScope, Result, RuntimeError};
use ironmq_codec::frame;
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

pub(crate) async fn declare(exchanges: &mut Exchanges, args: frame::ExchangeDeclareArgs) -> Result<()> {
    let passive = args.flags.contains(frame::ExchangeDeclareFlags::PASSIVE);
    let durable = args.flags.contains(frame::ExchangeDeclareFlags::DURABLE);
    let auto_delete = args.flags.contains(frame::ExchangeDeclareFlags::AUTO_DELETE);
    let internal = args.flags.contains(frame::ExchangeDeclareFlags::INTERNAL);

    match exchanges.exchanges.get(&args.exchange_name) {
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
                    args.exchange_name,
                    args.exchange_type,
                    durable,
                    auto_delete,
                    internal
                )
                .await?;

                Ok(())
            }
        }
        Some(ex) => {
            if passive {
                Ok(())
            } else {
                if ex.exchange_type == args.exchange_type
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
        exchange_loop(&mut receiver).await;
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
        let mut exchanges = start();
        let mut args = frame::ExchangeDeclareArgs::default();

        args.exchange_name = "new exchange".to_string();
        args.flags |= frame::ExchangeDeclareFlags::PASSIVE;

        let result = declare(&mut exchanges, args).await;

        assert!(result.is_err());
        let err = result.unwrap_err().downcast::<RuntimeError>().unwrap();
        assert_eq!(err.code, 404);
    }

    #[tokio::test]
    async fn declare_exchange_exists_fields_different_error() {
        let exchange_name = "orders".to_string();
        let exchange_type = "fanout".to_string();

        let mut exchanges = start();
        let (sender, _receiver) = mpsc::channel(1);
        let mut args = frame::ExchangeDeclareArgs::default();

        args.exchange_name = exchange_name.clone();
        args.exchange_type = exchange_type.clone();

        exchanges.exchanges.insert(
            exchange_name.clone(),
            Exchange {
                name: exchange_name.clone(),
                exchange_type: exchange_type.clone(),
                durable: false,
                auto_delete: true,
                internal: false,
                input: sender,
            },
        );

        let result = declare(&mut exchanges, args).await;

        assert!(result.is_err());

        let err = result.unwrap_err().downcast::<RuntimeError>().unwrap();
        assert_eq!(err.scope, ErrorScope::Channel);
        assert_eq!(err.code, conn_state::PRECONDITION_FAILED);
    }

    #[tokio::test]
    async fn declare_exchange_does_not_exist_created() {
        let mut exchanges = start();
        let exchange_name = "orders".to_string();
        let mut args = frame::ExchangeDeclareArgs::default();
        args.exchange_name = exchange_name.clone();
        args.flags |= frame::ExchangeDeclareFlags::DURABLE;
        args.flags |= frame::ExchangeDeclareFlags::AUTO_DELETE;

        let result = declare(&mut exchanges, args).await;

        assert!(result.is_ok());

        let ex = exchanges.exchanges.get(&exchange_name).unwrap();
        assert_eq!(ex.name, exchange_name);
        assert_eq!(ex.durable, true);
        assert_eq!(ex.auto_delete, true);
        assert_eq!(ex.internal, false);
    }
}
