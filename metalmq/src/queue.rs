pub(crate) mod handler;
pub(crate) mod manager;

use metalmq_codec::frame::{QueueDeclareArgs, QueueDeclareFlags};

/// Representation of a queue.
#[derive(Clone, Debug)]
pub(crate) struct Queue {
    /// The name aka the identifier of the queue.
    name: String,
    /// Durable queue remains active when server restarts.
    durable: bool,
    /// Exclusive queues can be accessed by the declaring connection.
    exclusive: bool,
    /// Queue is deleted when all consumers cancelled on it.
    auto_delete: bool,
}

impl Default for Queue {
    fn default() -> Queue {
        Queue {
            name: "default".to_string(),
            durable: false,
            exclusive: false,
            auto_delete: false,
        }
    }
}

impl From<QueueDeclareArgs> for Queue {
    fn from(f: QueueDeclareArgs) -> Self {
        Queue {
            name: f.name,
            durable: QueueDeclareFlags::contains(&f.flags, QueueDeclareFlags::DURABLE),
            exclusive: QueueDeclareFlags::contains(&f.flags, QueueDeclareFlags::EXCLUSIVE),
            auto_delete: QueueDeclareFlags::contains(&f.flags, QueueDeclareFlags::AUTO_DELETE),
        }
    }
}
