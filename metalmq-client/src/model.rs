use metalmq_codec::frame;

/// AMQP channel number
pub type ChannelNumber = frame::Channel;
/// AMQP method class id
pub type ClassId = frame::ClassId;
/// AMQP class id method id number
pub type ClassMethod = frame::ClassMethod;

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
