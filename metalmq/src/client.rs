pub(crate) mod conn;
pub(crate) mod state;

use crate::{ErrorScope, Result, RuntimeError};
use metalmq_codec::frame;

#[derive(Debug)]
pub(crate) enum ConnectionError {
    ConnectionForced = 320,
    InvalidPath = 402,
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
pub(crate) enum ChannelError {
    ContentTooLarge = 311,
    NoRoute = 312,
    NoConsumers = 313,
    AccessRefused = 403,
    NotFound = 404,
    ResourceLocked = 405,
    PreconditionFailed = 406,
}

/// Helper to create connection error frames.
pub(crate) fn connection_error<T>(cm: u32, code: ConnectionError, text: &str) -> Result<T> {
    let (class_id, method_id) = frame::split_class_method(cm);

    Err(Box::new(RuntimeError {
        scope: ErrorScope::Connection,
        channel: 0,
        code: code as u16,
        text: text.to_string(),
        class_id,
        method_id,
    }))
}

/// Helper to create channel error frames.
pub(crate) fn channel_error<T>(channel: frame::Channel, cm: u32, code: ChannelError, text: &str) -> Result<T> {
    let (class_id, method_id) = frame::split_class_method(cm);

    Err(Box::new(RuntimeError {
        scope: ErrorScope::Channel,
        channel,
        code: code as u16,
        text: text.to_string(),
        class_id,
        method_id,
    }))
}
