use super::{AMQPFrame, Channel, FieldTable, MethodFrameArgs};

bitflags! {
    pub struct ExchangeDeclareFlags: u8 {
        const PASSIVE = 0b00000001;
        const DURABLE = 0b00000010;
        const AUTO_DELETE = 0b00000100;
        const INTERNAL = 0b00001000;
        const NO_WAIT = 0b00010000;
    }
}

impl Default for ExchangeDeclareFlags {
    fn default() -> Self {
        ExchangeDeclareFlags::empty()
    }
}

#[derive(Debug, Default)]
pub struct ExchangeDeclareArgs {
    pub exchange_name: String,
    pub exchange_type: String,
    pub flags: ExchangeDeclareFlags,
    pub args: Option<FieldTable>,
}

#[allow(unused)]
impl ExchangeDeclareArgs {
    pub fn exchange_name(mut self, name: &str) -> Self {
        self.exchange_name = name.to_string();
        self
    }

    // type can be here an enum
    pub fn exchange_type(mut self, exchange_type: &str) -> Self {
        self.exchange_type = exchange_type.to_string();
        self
    }

    pub fn auto_delete(mut self, mode: bool) -> Self {
        self.flags.set(ExchangeDeclareFlags::AUTO_DELETE, mode);
        self
    }

    pub fn durable(mut self, mode: bool) -> Self {
        self.flags.set(ExchangeDeclareFlags::DURABLE, mode);
        self
    }

    pub fn internal(mut self, mode: bool) -> Self {
        self.flags.set(ExchangeDeclareFlags::INTERNAL, mode);
        self
    }

    pub fn passive(mut self, mode: bool) -> Self {
        self.flags.set(ExchangeDeclareFlags::PASSIVE, mode);
        self
    }

    pub fn frame(self, channel: Channel) -> AMQPFrame {
        AMQPFrame::Method(
            channel,
            super::EXCHANGE_DECLARE,
            super::MethodFrameArgs::ExchangeDeclare(self),
        )
    }
}

bitflags! {
    pub struct ExchangeDeleteFlags: u8 {
        const IF_UNUSED = 0b00000001;
        const NO_WAIT = 0b00000010;
    }
}

impl Default for ExchangeDeleteFlags {
    fn default() -> Self {
        ExchangeDeleteFlags::empty()
    }
}

#[derive(Debug, Default)]
pub struct ExchangeDeleteArgs {
    pub exchange_name: String,
    pub flags: ExchangeDeleteFlags,
}

impl ExchangeDeleteArgs {
    pub fn exchange_name(mut self, name: &str) -> Self {
        self.exchange_name = name.to_string();
        self
    }

    pub fn if_unused(mut self, mode: bool) -> Self {
        self.flags.set(ExchangeDeleteFlags::IF_UNUSED, mode);
        self
    }

    pub fn frame(self, channel: Channel) -> AMQPFrame {
        AMQPFrame::Method(
            channel,
            super::EXCHANGE_DELETE,
            super::MethodFrameArgs::ExchangeDelete(self),
        )
    }
}

pub fn exchange_declare_ok(channel: Channel) -> AMQPFrame {
    AMQPFrame::Method(channel, super::EXCHANGE_DECLARE_OK, MethodFrameArgs::ExchangeDeclareOk)
}

pub fn exchange_delete_ok(channel: Channel) -> AMQPFrame {
    AMQPFrame::Method(channel, super::EXCHANGE_DELETE_OK, MethodFrameArgs::ExchangeDeleteOk)
}
