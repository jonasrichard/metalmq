pub mod conn;
pub mod state;

use crate::{ErrorScope, Result, RuntimeError};
use metalmq_codec::codec::Frame;
use metalmq_codec::frame::{self, Channel};

#[derive(Debug)]
pub enum ConnectionError {
    ConnectionForced = 320,
    InvalidPath = 402,
    AccessRefused = 403,
    FrameError = 501,
    SyntaxError = 502,
    CommandInvalid = 503,
    ChannelError = 504,
    UnexpectedFrame = 505,
    ResourceError = 506,
    NotAllowed = 530,
    NotImplemented = 540,
    InternalError = 541,
}

#[derive(Debug)]
pub enum ChannelError {
    ContentTooLarge = 311,
    NoRoute = 312,
    NoConsumers = 313,
    AccessRefused = 403,
    NotFound = 404,
    ResourceLocked = 405,
    PreconditionFailed = 406,
}

/// Helper to create connection error frames.
pub fn connection_error<T>(cm: u32, code: ConnectionError, text: &str) -> Result<T> {
    Err(Box::new(RuntimeError {
        scope: ErrorScope::Connection,
        channel: 0,
        code: code as u16,
        text: text.to_owned(),
        class_method: cm,
    }))
}

/// Convert ConnectionError to connection close frame.
pub fn connection_error_frame(cm: u32, code: ConnectionError, text: &str) -> Frame {
    Frame::Frame(frame::connection_close(code as u16, text, cm))
}

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

/// Helper to create channel error frames.
pub fn channel_error<T>(channel: Channel, cm: u32, code: ChannelError, text: &str) -> Result<T> {
    Err(Box::new(RuntimeError {
        scope: ErrorScope::Channel,
        channel,
        code: code as u16,
        text: text.to_owned(),
        class_method: cm,
    }))
}

/// Convert ChannelError to channel close frame.
pub fn channel_error_frame(channel: Channel, cm: u32, code: ChannelError, text: &str) -> Frame {
    let (class_id, method_id) = frame::split_class_method(cm);

    Frame::Frame(frame::channel_close(channel, code as u16, text, cm))
}

pub fn runtime_error_to_frame(rte: &RuntimeError) -> Frame {
    let amqp_frame = match rte.scope {
        ErrorScope::Connection => frame::connection_close(rte.code, &rte.text, rte.class_method),
        ErrorScope::Channel => frame::channel_close(rte.channel, rte.code, &rte.text, rte.class_method),
    };

    Frame::Frame(amqp_frame)
}

pub fn to_runtime_error(err: Box<dyn std::error::Error>) -> RuntimeError {
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
