use bitflags::BitFlags;

use super::{AMQPFrame, Channel, FieldTable, MethodFrameArgs};

bitflags! {
    #[derive(Debug)]
    pub struct QueueDeclareFlags: u8 {
        const PASSIVE = 0b00000001;
        const DURABLE = 0b00000010;
        const EXCLUSIVE = 0b00000100;
        const AUTO_DELETE = 0b00001000;
        const NO_WAIT = 0b00010000;
    }
}

impl Default for QueueDeclareFlags {
    fn default() -> Self {
        QueueDeclareFlags::empty()
    }
}

#[derive(Debug, Default)]
pub struct QueueDeclareArgs {
    pub name: String,
    pub flags: QueueDeclareFlags,
    pub args: Option<FieldTable>,
}

impl QueueDeclareArgs {
    pub fn name(mut self, name: &str) -> Self {
        self.name = name.to_string();
        self
    }

    pub fn auto_delete(mut self, mode: bool) -> Self {
        BitFlags::set(&mut self.flags, QueueDeclareFlags::AUTO_DELETE, mode);
        self
    }

    pub fn durable(mut self, mode: bool) -> Self {
        BitFlags::set(&mut self.flags, QueueDeclareFlags::DURABLE, mode);
        self
    }

    pub fn exclusive(mut self, mode: bool) -> Self {
        BitFlags::set(&mut self.flags, QueueDeclareFlags::EXCLUSIVE, mode);
        self
    }

    pub fn passive(mut self, mode: bool) -> Self {
        BitFlags::set(&mut self.flags, QueueDeclareFlags::PASSIVE, mode);
        self
    }

    pub fn frame(self, channel: Channel) -> AMQPFrame {
        AMQPFrame::Method(channel, super::QUEUE_DECLARE, MethodFrameArgs::QueueDeclare(self))
    }
}

#[derive(Debug, Default)]
pub struct QueueDeclareOkArgs {
    pub name: String,
    pub message_count: u32,
    pub consumer_count: u32,
}

impl QueueDeclareOkArgs {
    pub fn name(mut self, name: &str) -> Self {
        self.name = name.to_string();
        self
    }

    pub fn consumer_count(mut self, consumer_count: u32) -> Self {
        self.consumer_count = consumer_count;
        self
    }

    pub fn message_count(mut self, message_count: u32) -> Self {
        self.message_count = message_count;
        self
    }

    pub fn frame(self, channel: Channel) -> AMQPFrame {
        AMQPFrame::Method(channel, super::QUEUE_DECLARE_OK, MethodFrameArgs::QueueDeclareOk(self))
    }
}

#[derive(Debug, Default)]
pub struct QueueBindArgs {
    pub queue_name: String,
    pub exchange_name: String,
    pub routing_key: String,
    pub no_wait: bool,
    pub args: Option<FieldTable>,
}

impl QueueBindArgs {
    pub fn new(queue_name: &str, exchange_name: &str) -> Self {
        Self {
            queue_name: queue_name.to_string(),
            exchange_name: exchange_name.to_string(),
            ..Default::default()
        }
    }

    pub fn routing_key(mut self, routing_key: &str) -> Self {
        self.routing_key = routing_key.to_string();
        self
    }

    pub fn frame(self, channel: Channel) -> AMQPFrame {
        AMQPFrame::Method(channel, super::QUEUE_BIND, MethodFrameArgs::QueueBind(self))
    }
}

#[derive(Debug, Default)]
pub struct QueuePurgeArgs {
    pub queue_name: String,
    pub no_wait: bool,
}

impl QueuePurgeArgs {
    pub fn queue_name(mut self, queue_name: &str) -> Self {
        self.queue_name = queue_name.to_string();
        self
    }

    pub fn frame(self, channel: Channel) -> AMQPFrame {
        AMQPFrame::Method(channel, super::QUEUE_PURGE, MethodFrameArgs::QueuePurge(self))
    }
}

#[derive(Debug, Default)]
pub struct QueuePurgeOkArgs {
    pub message_count: u32,
}

impl QueuePurgeOkArgs {
    pub fn message_count(mut self, message_count: u32) -> Self {
        self.message_count = message_count;
        self
    }

    pub fn frame(self, channel: Channel) -> AMQPFrame {
        AMQPFrame::Method(channel, super::QUEUE_PURGE_OK, MethodFrameArgs::QueuePurgeOk(self))
    }
}

bitflags! {
    #[derive(Debug)]
    pub struct QueueDeleteFlags: u8 {
        const IF_UNUSED = 0b00000001;
        const IF_EMPTY = 0b00000010;
        const NO_WAIT = 0b00000100;
    }
}

impl Default for QueueDeleteFlags {
    fn default() -> Self {
        QueueDeleteFlags::empty()
    }
}

#[derive(Debug, Default)]
pub struct QueueDeleteArgs {
    pub queue_name: String,
    pub flags: QueueDeleteFlags,
}

impl QueueDeleteArgs {
    pub fn queue_name(mut self, queue_name: &str) -> Self {
        self.queue_name = queue_name.to_string();
        self
    }

    pub fn if_empty(mut self, mode: bool) -> Self {
        BitFlags::set(&mut self.flags, QueueDeleteFlags::IF_EMPTY, mode);
        self
    }

    pub fn if_unused(mut self, mode: bool) -> Self {
        BitFlags::set(&mut self.flags, QueueDeleteFlags::IF_UNUSED, mode);
        self
    }

    pub fn frame(self, channel: Channel) -> AMQPFrame {
        AMQPFrame::Method(channel, super::QUEUE_DELETE, MethodFrameArgs::QueueDelete(self))
    }
}

#[derive(Debug, Default)]
pub struct QueueDeleteOkArgs {
    pub message_count: u32,
}

impl QueueDeleteOkArgs {
    pub fn message_count(mut self, message_count: u32) -> Self {
        self.message_count = message_count;
        self
    }

    pub fn frame(self, channel: Channel) -> AMQPFrame {
        AMQPFrame::Method(channel, super::QUEUE_DELETE_OK, MethodFrameArgs::QueueDeleteOk(self))
    }
}

#[derive(Debug, Default)]
pub struct QueueUnbindArgs {
    pub queue_name: String,
    pub exchange_name: String,
    pub routing_key: String,
    pub args: Option<FieldTable>,
}

impl QueueUnbindArgs {
    pub fn new(queue_name: &str, exchange_name: &str) -> Self {
        Self {
            queue_name: queue_name.to_string(),
            exchange_name: exchange_name.to_string(),
            ..Default::default()
        }
    }

    pub fn routing_key(mut self, routing_key: &str) -> Self {
        self.routing_key = routing_key.to_string();
        self
    }

    pub fn frame(self, channel: Channel) -> AMQPFrame {
        AMQPFrame::Method(channel, super::QUEUE_UNBIND, MethodFrameArgs::QueueUnbind(self))
    }
}

pub fn queue_bind_ok(channel: Channel) -> AMQPFrame {
    AMQPFrame::Method(channel, super::QUEUE_BIND_OK, MethodFrameArgs::QueueBindOk)
}
