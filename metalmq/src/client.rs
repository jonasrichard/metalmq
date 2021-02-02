pub(crate) mod conn;
pub(crate) mod state;

use crate::{ErrorScope, Result, RuntimeError};
use metalmq_codec::frame;

/// Helper to create channel error frames.
pub(crate) fn error<T>(channel: frame::Channel, cm: u32, code: u16, text: &str) -> Result<T> {
    let (class_id, method_id) = frame::split_class_method(cm);

    Err(Box::new(RuntimeError {
        scope: ErrorScope::Channel,
        channel: channel,
        code: code,
        text: text.to_string(),
        class_id: class_id,
        method_id: method_id
    }))
}
