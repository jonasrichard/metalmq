use metalmq_codec::codec::Frame;

use crate::{ErrorScope, Result, RuntimeError};

// TODO here goes the connection related business logic
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
    metalmq_codec::codec::Frame::Frame(metalmq_codec::frame::connection_close(code as u16, text, cm))
}
