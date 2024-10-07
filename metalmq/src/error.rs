use std::fmt;

use metalmq_codec::{
    codec::Frame,
    frame::{self, AMQPFrame, MethodFrameArgs},
};

/// The own result type where the error part is a async friendly error.
pub type Result<T> = std::result::Result<T, Error>;

/// Shorthand of a boxed Send, Sync error.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// Connection scope errors
#[derive(Debug)]
pub enum ConnectionError {
    /// The server forced to close the connection.
    ConnectionForced = 320,
    /// The client tried to work with an invalid virtual host.
    InvalidPath = 402,
    /// The client tried to access a resource it didn't have access.
    AccessRefused = 403,
    /// The client sent an invalid AMQP frame.
    FrameError = 501,
    /// The client sent a frame which contains erroneous data.
    SyntaxError = 502,
    /// The client sent a frame which didn't fit in the normal order.
    CommandInvalid = 503,
    /// The client tried to access a non-existing or not-opened channel.
    ChannelError = 504,
    /// The client sent an unexpected content header or body frame.
    UnexpectedFrame = 505,
    /// The client tried to exceed the limits of the connection agreed during connection tune.
    ResourceError = 506,
    /// The client tried to work with an entity in a way which is not allowed by the server.
    NotAllowed = 530,
    /// The client tried to use a not implemented funcionality.
    NotImplemented = 540,
    /// The server couldn't fulfill the request because of an intermittent error.
    InternalError = 541,
}

/// Channel scope errors
#[derive(Debug)]
pub enum ChannelError {
    /// Denotes successful execution like connection or channel closed.
    Success = 200,
    /// The client attempted to transfer a message which exceeded the limits.
    ContentTooLarge = 311,
    /// The mandatory message cannot be routed to queues.
    NoRoute = 312,
    /// The immediate message cannot be delivered to consumers in the absence of consumers.
    NoConsumers = 313,
    /// The client tried to access a resource it didn't have access.
    AccessRefused = 403,
    /// Queue or entity cannot be found.
    NotFound = 404,
    /// The client cannot access a resource because another client is working on what.
    ResourceLocked = 405,
    /// The work on resource is refused mostly because of validation errors.
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
