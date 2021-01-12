pub(crate) mod handler;
pub(crate) mod manager;

/// Representation of a queue.
pub(crate) struct Queue {
    /// The name aka the identifier of the queue.
    name: String,
    /// The channel via one can send commands/messages to the queue.
    command_sink: handler::QueueCommandSink
}
