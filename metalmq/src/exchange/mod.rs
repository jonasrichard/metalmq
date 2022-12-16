//! Exchanges Handle incoming messages and forward them to other exhanges
//! or queues. Each exchange is a lightweight process and handle messages
//! through a channel. When a client is publishing to an exchange it should
//! clone the exchange channel, so the messages will be handled serially.

pub mod binding;
pub mod handler;
pub mod manager;

use crate::client::{self, ConnectionError};
use crate::Result;
use metalmq_codec::frame::{self, ExchangeDeclareArgs, ExchangeDeclareFlags};
use serde_derive::Serialize;
use std::str::FromStr;

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum ExchangeType {
    Direct,
    Topic,
    Fanout,
    Headers,
}

/// Descriptive information of the exchanges
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Exchange {
    name: String,
    exchange_type: ExchangeType,
    durable: bool,
    auto_delete: bool,
    internal: bool,
}

/// Convert String to ExchangeType
impl FromStr for ExchangeType {
    type Err = ();

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "direct" => Ok(ExchangeType::Direct),
            "topic" => Ok(ExchangeType::Topic),
            "fanout" => Ok(ExchangeType::Fanout),
            "headers" => Ok(ExchangeType::Headers),
            _ => Err(()),
        }
    }
}

pub fn validate_exchange_type(exchange_type: &str) -> Result<()> {
    match ExchangeType::from_str(exchange_type) {
        Ok(_) => Ok(()),
        Err(_) => client::connection_error(
            frame::EXCHANGE_DECLARE,
            ConnectionError::CommandInvalid,
            "COMMAND_INVALID - Exchange type is invalid",
        ),
    }
}

impl Default for Exchange {
    fn default() -> Exchange {
        Exchange {
            name: "default".to_string(),
            exchange_type: ExchangeType::Direct,
            durable: false,
            auto_delete: false,
            internal: false,
        }
    }
}

impl From<ExchangeDeclareArgs> for Exchange {
    fn from(f: ExchangeDeclareArgs) -> Self {
        Exchange {
            name: f.exchange_name,
            exchange_type: ExchangeType::from_str(&f.exchange_type).unwrap(),
            durable: ExchangeDeclareFlags::contains(&f.flags, ExchangeDeclareFlags::DURABLE),
            auto_delete: ExchangeDeclareFlags::contains(&f.flags, ExchangeDeclareFlags::AUTO_DELETE),
            internal: ExchangeDeclareFlags::contains(&f.flags, ExchangeDeclareFlags::INTERNAL),
        }
    }
}
