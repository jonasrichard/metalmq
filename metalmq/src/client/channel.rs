use metalmq_codec::{codec::Frame, frame};
use types::ChannelError;

use crate::{ErrorScope, Result, RuntimeError};

pub mod basic;
pub mod content;
pub mod exchange;
pub mod open_close;
pub mod queue;
pub mod types;

// TODO I left the error related codes here because they will go to an error handler module anyway.

/// Helper to create channel error frames.
pub fn channel_error<T>(channel: u16, cm: u32, code: ChannelError, text: &str) -> Result<T> {
    Err(Box::new(RuntimeError {
        scope: ErrorScope::Channel,
        channel,
        code: code as u16,
        text: text.to_owned(),
        class_method: cm,
    }))
}

// TODO move all error converstion to an error mod
pub fn runtime_error_to_frame(rte: &RuntimeError) -> Frame {
    let amqp_frame = match rte.scope {
        ErrorScope::Connection => frame::connection_close(rte.code, &rte.text, rte.class_method),
        ErrorScope::Channel => frame::channel_close(rte.channel, rte.code, &rte.text, rte.class_method),
    };

    Frame::Frame(amqp_frame)
}
