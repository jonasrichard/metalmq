use metalmq_codec::frame;

/// AMQP channel number
pub type ChannelNumber = frame::Channel;
/// AMQP method class id
pub type ClassId = frame::ClassId;
/// AMQP class id method id number
pub type ClassMethod = frame::ClassMethod;

/// Error codes in connection scope.
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

/// Error codes in channel scope.
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
