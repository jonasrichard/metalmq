pub mod channel;
pub mod conn;
pub mod connection;
pub mod state;

use connection::ConnectionError;

use crate::{ErrorScope, RuntimeError};

//pub fn connection_error_frame(err: RuntimeError) -> Option<Frame> {
//    if err.scope == ErrorScope::Channel {
//        return None;
//    }
//
//    Some(Frame::Frame(frame::connection_close(
//        0,
//        err.code,
//        &err.text,
//        err.class_id,
//        err.method_id,
//    )))
//}

/// Converts all errors as `RuntimeError`. Unknown errors are wrapped as internal connection
/// errors.
pub fn to_runtime_error(err: Box<dyn std::error::Error + Send + Sync>) -> RuntimeError {
    match err.downcast::<RuntimeError>() {
        Ok(rte) => *rte,
        Err(e) => RuntimeError {
            scope: ErrorScope::Connection,
            channel: 0,
            code: ConnectionError::InternalError as u16,
            text: format!("Internal error: {e}"),
            class_method: 0,
        },
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::RuntimeError;
    use crate::Result;

    /// Helper for tests to extract the `Err` as `RuntimeError`
    pub(crate) fn to_runtime_error<T: std::fmt::Debug>(result: Result<T>) -> RuntimeError {
        *result.unwrap_err().downcast::<RuntimeError>().unwrap()
    }
}
