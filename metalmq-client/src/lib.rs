//! AMQP 0.9 compatible async client based on Tokio.
//!
//! # Usage
//!
//! Add the following to your `Cargo.toml`
//!
//! ```toml
//! [dependencies]
//! metalmq-client = "0.2.2"
//! ```
//!
//! And then from an async function you can connect to an AMQP server.
//!
//! ```
//! use metalmq_client::Client;
//!
//! async fn send_message() {
//!     let mut client = Client::connect("localhost:5672", "guest", "guest").await.unwrap();
//!
//!     client.channel_open(1).await.unwrap();
//!     client.close().await.unwrap();
//! }
//! ```
mod dev;
pub use dev::setup_logger;

mod channel_api;
pub use channel_api::{
    Binding, Channel, ExchangeDeclareOpts, ExchangeType, HeaderMatch, IfEmpty, IfUnused, QueueDeclareOpts,
};

mod client_api;
pub use client_api::{Client, EventSignal};

mod consumer;
pub use consumer::{ConsumerHandler, ConsumerSignal, Exclusive, NoAck, NoLocal};

mod error;
pub use error::ClientError;

mod message;
pub use message::{Content, DeliveredMessage, MessageProperties, PublishedMessage, ReturnedMessage};

mod model;
pub use model::ChannelNumber;

mod processor;
mod state;
