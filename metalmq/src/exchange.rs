//! Exchanges Handle incoming messages and forward them to other exhanges
//! or queues. Each exchange is a lightweight process and handle messages
//! through a channel. When a client is publishing to an exchange it should
//! clone the exchange channel, so the messages will be handled serially.

pub(crate) mod handler;
pub(crate) mod manager;

use metalmq_codec::frame::{ExchangeDeclareArgs, ExchangeDeclareFlags};
use serde::Serialize;

#[derive(Clone, Debug, PartialEq, Serialize)]
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
