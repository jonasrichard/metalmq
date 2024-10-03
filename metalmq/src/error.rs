use std::fmt;

use metalmq_codec::{
    codec::Frame,
    frame::{self, AMQPFrame, MethodFrameArgs},
};

/// The own result type where the error part is a async friendly error.
pub type Result<T> = std::result::Result<T, Error>;

/// Shorthand of a boxed Send, Sync error.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

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

#[derive(Debug, Default, PartialEq)]
pub enum ErrorScope {
    #[default]
    Connection,
    Channel,
}

#[derive(Debug, Default)]
pub struct RuntimeError {
    pub scope: ErrorScope,
    pub channel: metalmq_codec::frame::Channel,
    pub code: u16,
    pub text: String,
    pub class_method: u32,
}

impl From<RuntimeError> for AMQPFrame {
    fn from(err: RuntimeError) -> AMQPFrame {
        match err.scope {
            ErrorScope::Connection => metalmq_codec::frame::connection_close(err.code, &err.text, err.class_method),
            ErrorScope::Channel => {
                metalmq_codec::frame::channel_close(err.channel, err.code, &err.text, err.class_method)
            }
        }
    }
}

impl From<RuntimeError> for Frame {
    fn from(value: RuntimeError) -> Self {
        let f: AMQPFrame = value.into();

        Frame::Frame(f)
    }
}

impl<T> From<RuntimeError> for Result<T> {
    fn from(value: RuntimeError) -> Self {
        Err(Box::new(value))
    }
}

impl From<AMQPFrame> for RuntimeError {
    fn from(value: AMQPFrame) -> Self {
        match value {
            AMQPFrame::Method(0, _, MethodFrameArgs::ConnectionClose(args)) => RuntimeError {
                scope: ErrorScope::Connection,
                channel: 0,
                code: args.code,
                text: args.text.to_string(),
                class_method: frame::unify_class_method(args.class_id, args.method_id),
            },
            AMQPFrame::Method(channel, _, MethodFrameArgs::ChannelClose(args)) => RuntimeError {
                scope: ErrorScope::Channel,
                channel,
                code: args.code,
                text: args.text.to_string(),
                class_method: frame::unify_class_method(args.class_id, args.method_id),
            },
            f => panic!("Unknown frame {f:?}"),
        }
    }
}

impl std::fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for RuntimeError {}

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

impl ConnectionError {
    pub fn into_frame(self, cm: u32, text: &str) -> Frame {
        Frame::Frame(metalmq_codec::frame::connection_close(self as u16, text, cm))
    }

    pub fn into_runtime_error(self, cm: u32, text: &str) -> RuntimeError {
        RuntimeError {
            scope: ErrorScope::Connection,
            channel: 0,
            code: self as u16,
            text: text.to_owned(),
            class_method: cm,
        }
    }

    pub fn into_result<T>(self, cm: u32, text: &str) -> Result<T> {
        self.into_runtime_error(cm, text).into()
    }
}

impl ChannelError {
    pub fn into_frame(self, channel: u16, cm: u32, text: &str) -> Frame {
        Frame::Frame(metalmq_codec::frame::channel_close(channel, self as u16, text, cm))
    }

    pub fn into_runtime_error(self, channel: u16, cm: u32, text: &str) -> RuntimeError {
        RuntimeError {
            scope: ErrorScope::Channel,
            channel,
            code: self as u16,
            text: text.to_owned(),
            class_method: cm,
        }
    }

    pub fn into_result<T>(self, channel: u16, cm: u32, text: &str) -> Result<T> {
        self.into_runtime_error(channel, cm, text).into()
    }
}

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
