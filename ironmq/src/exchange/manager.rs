use crate::Result;
use crate::client::state;
use crate::exchange::{error, Exchange};
use crate::exchange::handler::{self, ExchangeChannel, ManagerCommand};
use crate::queue::handler::QueueChannel;
use ironmq_codec::frame;
use log::{debug, error};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};

pub(crate) struct Exchanges {
    mutex : Arc<Mutex<()>>,
    control: handler::ControlChannel,
    exchanges: HashMap<String, Exchange>,
}

#[async_trait]
pub(crate) trait ExchangeManager: Sync + Send {
    async fn declare(&mut self, exchange: Exchange, passive: bool, conn: &str) -> Result<ExchangeChannel>;
    async fn bind_queue(&mut self, exchange_name: String, queue_channel: QueueChannel) -> Result<()>;
    async fn clone_connection(&mut self, exchange_name: &str, conn: &str) -> Result<()>;
}


pub(crate) fn start() -> Exchanges {
    let (sink, mut source) = mpsc::channel(1);

    tokio::spawn(async move {
        if let Err(e) = handler::exchange_manager_loop(&mut source).await {
            error!("Exchange manager loop finish with error {:?}", e);
        }
    });

    Exchanges {
        mutex: Arc::new(Mutex::new(())),
        control: sink,
        exchanges: HashMap::new(), // TODO add default exchanges from a config or db
    }
}

#[async_trait]
impl ExchangeManager for Exchanges {
    async fn declare(&mut self, exchange: Exchange, passive: bool, conn: &str) -> Result<ExchangeChannel> {
        let _ = self.mutex.lock();

        debug!("{:?}", self.exchanges);

        match self.exchanges.get(&exchange.name) {
            None =>
                if passive {
                    return error(0, frame::EXCHANGE_DECLARE, 404, "Exchange not found")
                },
            Some(current) => {
                debug!("Current instance {:?}", current);

                if *current != exchange {
                    error!("Current exchange: {:?} to be declared. {:?}", current, exchange);

                    return error(0, frame::EXCHANGE_DECLARE, state::PRECONDITION_FAILED,
                                 "Exchange exists but properties are different")
                }
            }
        }

        let channel = create_exchange(&self.control, &exchange.name, conn).await?;
        self.exchanges.insert(exchange.name.clone(), exchange);

        Ok(channel)
    }

    async fn bind_queue(&mut self, exchange_name: String, queue_channel: QueueChannel) -> Result<()> {
        let _ = self.mutex.lock();

        debug!("Queue bind: {}", exchange_name);

        self.control.send(ManagerCommand::QueueBind{ exchange_name: exchange_name, sink: queue_channel }).await?;

        Ok(())
    }

    async fn clone_connection(&mut self, exchange_name: &str, conn: &str) -> Result<()> {
        Ok(())
    }
}

async fn create_exchange(control: &mpsc::Sender<ManagerCommand>, name: &str, conn: &str) -> Result<ExchangeChannel> {
    let (tx, rx) = oneshot::channel();
    control.send(ManagerCommand::ExchangeClone {
        name: name.to_string(),
        connection_id: conn.to_string(),
        response: tx
    }).await?;
    let ch = rx.await?;

    Ok(ch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ErrorScope, RuntimeError};
    use ironmq_codec::frame::{ExchangeDeclareArgs, ExchangeDeclareFlags};

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
        assert_eq!(err.code, state::PRECONDITION_FAILED);
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

        let exchange = exchanges.exchanges.get(&exchange_name).unwrap();
        assert_eq!(exchange.name, exchange_name);
        assert_eq!(exchange.durable, true);
        assert_eq!(exchange.auto_delete, true);
        assert_eq!(exchange.internal, false);
    }
}
