use crate::model;
use std::fmt;

/// Represents a connection or channel error. If `channel` is `None` it is a
/// connection error.
#[derive(Clone, Debug)]
pub struct ClientError {
    pub channel: Option<model::ChannelNumber>,
    pub code: u16,
    pub message: String,
    pub class_method: model::ClassMethod,
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientError")
            .field("channel", &self.channel)
            .field("code", &self.code)
            .field("message", &self.message)
            .field("class_method", &format!("{:08X}", &self.class_method))
            .finish()
    }
}

impl std::error::Error for ClientError {}

/// Shorthand for creating errors in async functions.
#[macro_export]
macro_rules! client_error {
    ($channel:expr, $code:expr, $message:expr, $cm:expr) => {
        ::std::result::Result::Err(anyhow::Error::new($crate::error::ClientError {
            channel: $channel,
            code: $code,
            message: ::std::string::String::from($message),
            class_method: $cm,
        }))
    };
}
