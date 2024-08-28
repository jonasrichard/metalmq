use metalmq_codec::codec::Frame;
use tokio::sync::mpsc;

use super::{connection::types::Connection, RuntimeError};
use crate::{exchange, queue, Context, Result};

mod connect;

/// Helper for tests to extract the `Err` as `RuntimeError`
pub(crate) fn to_runtime_error<T: std::fmt::Debug>(result: Result<T>) -> RuntimeError {
    *result.unwrap_err().downcast::<RuntimeError>().unwrap()
}

pub(crate) struct TestContext {
    exchange_manager: exchange::manager::ExchangeManagerSink,
    queue_manager: queue::manager::QueueManagerSink,
    connection: Connection,
    frame_rx: mpsc::Receiver<Frame>,
}

impl TestContext {
    pub(crate) fn setup() -> TestContext {
        let ex_mgr = exchange::manager::start();
        let q_mgr = queue::manager::start(ex_mgr.clone());
        let ctx = Context {
            exchange_manager: ex_mgr.clone(),
            queue_manager: q_mgr.clone(),
        };
        let (frame_tx, frame_rx) = mpsc::channel(16);

        let conn = Connection::new(ctx, frame_tx);

        TestContext {
            exchange_manager: ex_mgr,
            queue_manager: q_mgr,
            connection: conn,
            frame_rx,
        }
    }
}
