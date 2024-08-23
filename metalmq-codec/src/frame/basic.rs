use bitflags::Flags;

use super::{AMQPFrame, Channel, FieldTable, MethodFrameArgs};

bitflags! {
    #[derive(Debug)]
    pub struct BasicConsumeFlags: u8 {
        const NO_LOCAL = 0b00000001;
        const NO_ACK = 0b00000010;
        const EXCLUSIVE = 0b00000100;
        const NO_WAIT = 0b00001000;
    }
}

impl Default for BasicConsumeFlags {
    fn default() -> Self {
        BasicConsumeFlags::empty()
    }
}

#[derive(Debug, Default)]
pub struct BasicConsumeArgs {
    pub queue: String,
    pub consumer_tag: String,
    pub flags: BasicConsumeFlags,
    pub args: Option<FieldTable>,
}

impl BasicConsumeArgs {
    pub fn queue(mut self, queue: &str) -> Self {
        self.queue = queue.to_string();
        self
    }

    pub fn consumer_tag(mut self, consumer_tag: &str) -> Self {
        self.consumer_tag = consumer_tag.to_string();
        self
    }

    pub fn exclusive(mut self, mode: bool) -> Self {
        Flags::set(&mut self.flags, BasicConsumeFlags::EXCLUSIVE, mode);
        self
    }

    pub fn no_ack(mut self, mode: bool) -> Self {
        Flags::set(&mut self.flags, BasicConsumeFlags::NO_ACK, mode);
        self
    }

    pub fn no_local(mut self, mode: bool) -> Self {
        Flags::set(&mut self.flags, BasicConsumeFlags::NO_LOCAL, mode);
        self
    }

    pub fn frame(self, channel: Channel) -> AMQPFrame {
        AMQPFrame::Method(channel, super::BASIC_CONSUME, MethodFrameArgs::BasicConsume(self))
    }
}

#[derive(Clone, Debug, Default)]
pub struct BasicConsumeOkArgs {
    pub consumer_tag: String,
}

impl BasicConsumeOkArgs {
    pub fn new(consumer_tag: &str) -> Self {
        Self {
            consumer_tag: consumer_tag.to_string(),
        }
    }

    pub fn frame(self, channel: Channel) -> AMQPFrame {
        AMQPFrame::Method(channel, super::BASIC_CONSUME_OK, MethodFrameArgs::BasicConsumeOk(self))
    }
}

#[derive(Clone, Debug, Default)]
pub struct BasicCancelArgs {
    pub consumer_tag: String,
    pub no_wait: bool,
}

impl BasicCancelArgs {
    pub fn new(consumer_tag: &str) -> Self {
        Self {
            consumer_tag: consumer_tag.to_string(),
            no_wait: false,
        }
    }

    pub fn frame(self, channel: Channel) -> AMQPFrame {
        AMQPFrame::Method(channel, super::BASIC_CANCEL, super::MethodFrameArgs::BasicCancel(self))
    }
}

#[derive(Clone, Debug, Default)]
pub struct BasicCancelOkArgs {
    pub consumer_tag: String,
}

impl BasicCancelOkArgs {
    pub fn new(consumer_tag: &str) -> Self {
        Self {
            consumer_tag: consumer_tag.to_string(),
        }
    }

    pub fn frame(self, channel: Channel) -> AMQPFrame {
        AMQPFrame::Method(
            channel,
            super::BASIC_CANCEL_OK,
            super::MethodFrameArgs::BasicCancelOk(self),
        )
    }
}

#[derive(Clone, Debug, Default)]
pub struct BasicGetArgs {
    pub queue: String,
    pub no_ack: bool,
}

impl BasicGetArgs {
    pub fn new(queue: &str) -> Self {
        Self {
            queue: queue.to_string(),
            no_ack: false,
        }
    }

    pub fn no_ack(mut self, mode: bool) -> Self {
        self.no_ack = mode;
        self
    }

    pub fn frame(self, channel: Channel) -> AMQPFrame {
        AMQPFrame::Method(channel, super::BASIC_GET, super::MethodFrameArgs::BasicGet(self))
    }
}

#[derive(Clone, Debug, Default)]
pub struct BasicGetOkArgs {
    pub delivery_tag: u64,
    pub redelivered: bool,
    pub exchange_name: String,
    pub routing_key: String,
    pub message_count: u32,
}

impl BasicGetOkArgs {
    pub fn new(delivery_tag: u64, exchange_name: &str) -> Self {
        Self {
            delivery_tag,
            exchange_name: exchange_name.to_string(),
            ..Default::default()
        }
    }

    pub fn redelivered(mut self, mode: bool) -> Self {
        self.redelivered = mode;
        self
    }

    pub fn routing_key(mut self, routing_key: &str) -> Self {
        self.routing_key = routing_key.to_string();
        self
    }

    pub fn message_count(mut self, message_count: u32) -> Self {
        self.message_count = message_count;
        self
    }

    pub fn frame(self, channel: Channel) -> AMQPFrame {
        AMQPFrame::Method(channel, super::BASIC_GET_OK, super::MethodFrameArgs::BasicGetOk(self))
    }
}

bitflags! {
    #[derive(Clone, Debug)]
    pub struct BasicPublishFlags: u8 {
        const MANDATORY = 0b00000001;
        const IMMEDIATE = 0b00000010;
    }
}

impl Default for BasicPublishFlags {
    fn default() -> Self {
        BasicPublishFlags::empty()
    }
}

#[derive(Clone, Debug, Default)]
pub struct BasicPublishArgs {
    pub exchange_name: String,
    pub routing_key: String,
    pub flags: BasicPublishFlags,
}

impl BasicPublishArgs {
    pub fn new(exchange_name: &str) -> Self {
        Self {
            exchange_name: exchange_name.to_string(),
            ..Default::default()
        }
    }

    pub fn routing_key(mut self, routing_key: &str) -> Self {
        self.routing_key = routing_key.to_string();
        self
    }

    pub fn immediate(mut self, mode: bool) -> Self {
        Flags::set(&mut self.flags, BasicPublishFlags::IMMEDIATE, mode);
        self
    }

    pub fn mandatory(mut self, mode: bool) -> Self {
        Flags::set(&mut self.flags, BasicPublishFlags::MANDATORY, mode);
        self
    }

    pub fn frame(self, channel: Channel) -> AMQPFrame {
        AMQPFrame::Method(
            channel,
            super::BASIC_PUBLISH,
            super::MethodFrameArgs::BasicPublish(self),
        )
    }

    pub fn is_mandatory(&self) -> bool {
        self.flags.contains(BasicPublishFlags::MANDATORY)
    }

    pub fn is_immediate(&self) -> bool {
        self.flags.contains(BasicPublishFlags::IMMEDIATE)
    }
}

#[derive(Clone, Debug, Default)]
pub struct BasicReturnArgs {
    pub reply_code: u16,
    pub reply_text: String,
    pub exchange_name: String,
    pub routing_key: String,
}

impl BasicReturnArgs {
    pub fn frame(self, channel: Channel) -> AMQPFrame {
        AMQPFrame::Method(channel, super::BASIC_RETURN, super::MethodFrameArgs::BasicReturn(self))
    }
}

#[derive(Clone, Debug, Default)]
pub struct BasicDeliverArgs {
    pub consumer_tag: String,
    pub delivery_tag: u64,
    pub redelivered: bool,
    pub exchange_name: String,
    pub routing_key: String,
}

impl BasicDeliverArgs {
    pub fn new(consumer_tag: &str, delivery_tag: u64, exchange_name: &str) -> Self {
        Self {
            consumer_tag: consumer_tag.to_string(),
            delivery_tag,
            exchange_name: exchange_name.to_string(),
            ..Default::default()
        }
    }

    pub fn redelivered(mut self, mode: bool) -> Self {
        self.redelivered = mode;
        self
    }

    pub fn routing_key(mut self, routing_key: &str) -> Self {
        self.routing_key = routing_key.to_string();
        self
    }

    pub fn frame(self, channel: Channel) -> AMQPFrame {
        AMQPFrame::Method(channel, super::BASIC_DELIVER, MethodFrameArgs::BasicDeliver(self))
    }
}

#[derive(Clone, Debug, Default)]
pub struct BasicAckArgs {
    pub delivery_tag: u64,
    pub multiple: bool,
}

impl BasicAckArgs {
    pub fn delivery_tag(mut self, delivery_tag: u64) -> Self {
        self.delivery_tag = delivery_tag;
        self
    }

    pub fn multiple(mut self, mode: bool) -> Self {
        self.multiple = mode;
        self
    }

    pub fn frame(self, channel: Channel) -> AMQPFrame {
        AMQPFrame::Method(channel, super::BASIC_ACK, super::MethodFrameArgs::BasicAck(self))
    }
}

#[derive(Clone, Debug, Default)]
pub struct BasicRejectArgs {
    pub delivery_tag: u64,
    pub requeue: bool,
}

impl BasicRejectArgs {
    pub fn delivery_tag(mut self, delivery_tag: u64) -> Self {
        self.delivery_tag = delivery_tag;
        self
    }

    pub fn requeue(mut self, mode: bool) -> Self {
        self.requeue = mode;
        self
    }

    pub fn frame(self, channel: Channel) -> AMQPFrame {
        AMQPFrame::Method(channel, super::BASIC_REJECT, super::MethodFrameArgs::BasicReject(self))
    }
}

bitflags! {
    #[derive(Clone, Debug)]
    pub struct BasicNackFlags: u8 {
        const MULTIPLE = 0b00000001;
        const REQUEUE = 0b00000010;
    }
}

impl Default for BasicNackFlags {
    fn default() -> Self {
        BasicNackFlags::empty()
    }
}

#[derive(Clone, Debug, Default)]
pub struct BasicNackArgs {
    pub delivery_tag: u64,
    pub flags: BasicNackFlags,
}

impl BasicNackArgs {
    pub fn delivery_tag(mut self, value: u64) -> Self {
        self.delivery_tag = value;
        self
    }

    pub fn multiple(mut self, value: bool) -> Self {
        Flags::set(&mut self.flags, BasicNackFlags::MULTIPLE, value);
        self
    }

    pub fn requeue(mut self, value: bool) -> Self {
        Flags::set(&mut self.flags, BasicNackFlags::REQUEUE, value);
        self
    }

    pub fn frame(self, channel: Channel) -> AMQPFrame {
        AMQPFrame::Method(channel, super::BASIC_NACK, MethodFrameArgs::BasicNack(self))
    }
}

#[derive(Clone, Debug, Default)]
pub struct ConfirmSelectArgs {
    pub no_wait: bool,
}

pub fn basic_get_empty(channel: Channel) -> AMQPFrame {
    AMQPFrame::Method(channel, super::BASIC_GET_EMPTY, MethodFrameArgs::BasicGetEmpty)
}

pub fn confirm_select(channel: Channel) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        super::CONFIRM_SELECT,
        MethodFrameArgs::ConfirmSelect(ConfirmSelectArgs { no_wait: false }),
    )
}

pub fn confirm_select_ok(channel: Channel) -> AMQPFrame {
    AMQPFrame::Method(channel, super::CONFIRM_SELECT_OK, MethodFrameArgs::ConfirmSelectOk)
}
