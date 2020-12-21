//! Exchanges Handle incoming messages and forward them to other exhanges
//! or queues. Each exchange is a lightweight process and handle messages
//! through a channel. When a client is publishing to an exchange it should
//! clone the exchange channel, so the messages will be handled serially.
use crate::{message, ErrorScope, Result, RuntimeError};
use crate::client::state;
use ironmq_codec::frame;
use log::{debug, error, info};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

type ExchangeChannel = mpsc::Sender<message::Message>;

#[derive(Debug, PartialEq)]
pub(crate) struct Exchange {
    name: String,
    exchange_type: String,
    durable: bool,
    auto_delete: bool,
    internal: bool
}

impl From<frame::ExchangeDeclareArgs> for Exchange {
    fn from(f: frame::ExchangeDeclareArgs) -> Self {
        Exchange {
            name: f.exchange_name,
            exchange_type: f.exchange_type,
            durable: frame::ExchangeDeclareFlags::contains(&f.flags, frame::ExchangeDeclareFlags::DURABLE),
            auto_delete: frame::ExchangeDeclareFlags::contains(&f.flags, frame::ExchangeDeclareFlags::AUTO_DELETE),
            internal: frame::ExchangeDeclareFlags::contains(&f.flags, frame::ExchangeDeclareFlags::INTERNAL)
        }
    }
}

#[derive(Debug)]
pub(crate) struct ExchangeState {
    exchange: Exchange,
    instance_count: u16,
    input: ExchangeChannel
}

pub(crate) struct Exchanges {
    mutex : Arc<Mutex<()>>,
    exchanges: HashMap<String, ExchangeState>,
}

#[async_trait]
pub(crate) trait ExchangeManager: Sync + Send {
    async fn declare(&mut self, exchange: Exchange, passive: bool) -> Result<ExchangeChannel>;
}

pub(crate) fn start() -> Exchanges {
    Exchanges {
        mutex: Arc::new(Mutex::new(())),
        exchanges: HashMap::new(), // TODO add default exchanges from a config or db
    }
}

#[async_trait]
impl ExchangeManager for Exchanges {
    async fn declare(&mut self, exchange: Exchange, passive: bool) -> Result<ExchangeChannel> {
        let _ = self.mutex.lock();

        debug!("{:?}", self.exchanges);

        match self.exchanges.get_mut(&exchange.name) {
            None =>
                if passive {
                    Err(Box::new(RuntimeError {
                        scope: ErrorScope::Channel,
                        code: 404,
                        text: "Exchange not found".into(),
                        ..Default::default()
                    }))
                } else {
                    let channel = create_exchange(&mut self.exchanges, exchange).await?;

                    Ok(channel)
                },
            Some(mut current) => {
                debug!("Current instance {}", current.instance_count);

                if passive {
                    current.instance_count += 1;
                    Ok(current.input.clone())
                } else {
                    if current.exchange != exchange {
                        error!("Current exchange: {:?} to be declared. {:?}", current.exchange, exchange);

                        Err(Box::new(RuntimeError {
                            scope: ErrorScope::Channel,
                            code: state::PRECONDITION_FAILED,
                            text: "Exchange exists but properties are different".into(),
                            ..Default::default()
                        }))
                    } else {
                        current.instance_count += 1;
                        Ok(current.input.clone())
                    }
                }
            }
        }
    }
}

async fn create_exchange(exchanges: &mut HashMap<String, ExchangeState>, exchange: Exchange) -> Result<ExchangeChannel> {
    let (sender, mut receiver) = mpsc::channel(1);
    let other = sender.clone();
    let result = other.clone();
    let exchange_name = exchange.name.clone();

    debug!("New exchange {:?}", exchange);

    let state = ExchangeState {
        exchange: exchange,
        instance_count: 1,
        input: other
    };

    exchanges.insert(exchange_name.clone(), state);

    tokio::spawn(async move {
        exchange_loop(&mut receiver).await;
        info!("Exchange processor for {} stop", exchange_name);
        // TODO this never runs since we always have a copy of the ExchangeChannel
    });

    Ok(result)
}

async fn exchange_loop(messages: &mut mpsc::Receiver<message::Message>) {
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

        let result = exchanges.declare(args.into(), true).await;

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
            ExchangeState {
                exchange: Exchange {
                    name: exchange_name.clone(),
                    exchange_type: exchange_type.clone(),
                    durable: false,
                    auto_delete: true,
                    internal: false
                },
                instance_count: 0,
                input: sender
            });

        let result = exchanges.declare(args.into(), false).await;

        assert!(result.is_err());

        let err = result.unwrap_err().downcast::<RuntimeError>().unwrap();
        assert_eq!(err.scope, ErrorScope::Channel);
        assert_eq!(err.code, state::PRECONDITION_FAILED);
    }

    #[tokio::test]
    async fn declare_exchange_does_not_exist_created() {
        let mut exchanges = start();
        let exchange_name = "orders".to_string();
        let mut args = frame::ExchangeDeclareArgs::default();
        args.exchange_name = exchange_name.clone();
        args.flags |= frame::ExchangeDeclareFlags::DURABLE;
        args.flags |= frame::ExchangeDeclareFlags::AUTO_DELETE;

        let result = exchanges.declare(args.into(), false).await;

        assert!(result.is_ok());

        let state = exchanges.exchanges.get(&exchange_name).unwrap();
        assert_eq!(state.exchange.name, exchange_name);
        assert_eq!(state.exchange.durable, true);
        assert_eq!(state.exchange.auto_delete, true);
        assert_eq!(state.exchange.internal, false);
    }
}
