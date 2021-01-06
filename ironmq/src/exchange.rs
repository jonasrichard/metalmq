//! Exchanges Handle incoming messages and forward them to other exhanges
//! or queues. Each exchange is a lightweight process and handle messages
//! through a channel. When a client is publishing to an exchange it should
//! clone the exchange channel, so the messages will be handled serially.

pub(crate) mod handler;
pub(crate) mod manager;

use crate::{ErrorScope, Result, RuntimeError};

use ironmq_codec::frame::{self, ExchangeDeclareArgs, ExchangeDeclareFlags};

#[derive(Debug, PartialEq)]
pub(crate) struct Exchange {
    name: String,
    exchange_type: String,
    durable: bool,
    auto_delete: bool,
    internal: bool
}

impl From<ExchangeDeclareArgs> for Exchange {
    fn from(f: ExchangeDeclareArgs) -> Self {
        Exchange {
            name: f.exchange_name,
            exchange_type: f.exchange_type,
            durable: ExchangeDeclareFlags::contains(&f.flags, ExchangeDeclareFlags::DURABLE),
            auto_delete: ExchangeDeclareFlags::contains(&f.flags, ExchangeDeclareFlags::AUTO_DELETE),
            internal: ExchangeDeclareFlags::contains(&f.flags, ExchangeDeclareFlags::INTERNAL)
        }
    }
}

pub(crate) fn error<T>(channel: frame::Channel, cm: u32, code: u16, text: &str) -> Result<T> {
    let (class_id, method_id) = frame::split_class_method(cm);

    Err(Box::new(RuntimeError {
        scope: ErrorScope::Channel,
        channel: channel,
        code: code,
        text: text.to_string(),
        class_id: class_id,
        method_id: method_id
    }))
}
