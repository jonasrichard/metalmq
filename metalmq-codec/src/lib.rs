//! Data structures and converter functions for dealing with AMQP frames.
//!
//! All the data types are in the `frame` module, the `codec` implements
//! the encoding and the decoding.
pub mod codec;
pub mod frame;

#[cfg(test)]
mod tests;

#[macro_use]
extern crate bitflags;

use std::fmt;

/// Type alias for a sync and send error.
pub type Error = Box<dyn std::error::Error + Send + Sync>;
/// Type alias for a simplified Result with Error.
pub type Result<T> = std::result::Result<T, Error>;

/// Error struct used by the crate.
#[derive(Debug)]
pub struct FrameError {
    pub code: u16,
    pub message: String,
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", &self)
    }
}

impl std::error::Error for FrameError {}

/// Shorthand for making errors with error code and error message.
///
/// ```no_run
/// use metalmq_codec::frame_error;
/// use metalmq_codec::FrameError;
/// use metalmq_codec::frame::AMQPValue;
///
/// fn as_string(val: AMQPValue) -> Result<String, Box<dyn std::error::Error>> {
///     if let AMQPValue::SimpleString(s) = val {
///         return Ok(s.clone())
///     }
///
///     frame_error!(10, "Value cannot be converted to string")
/// }
/// ```
#[macro_export]
macro_rules! frame_error {
    ($code:expr, $message:expr) => {
        ::std::result::Result::Err(Box::new($crate::FrameError {
            code: $code,
            message: ::std::string::String::from($message),
        }))
    };
}
