//! AMQP 0.9 compatible async client based on Tokio.
//!
//! # Usage
//!
//! Add the following to your `Cargo.toml`
//!
//! ```toml
//! [dependencies]
//! metalmq-client = "0.3.0"
//! ```
//!
//! And then from an async function you can connect to an AMQP server.
//!
//! ```
//! use metalmq_client::Client;
//!
//! async fn send_message() {
//!     let (mut client, handler) =
//!         Client::connect("localhost:5672", "guest", "guest").await.unwrap();
//!
//!     client.channel_open(1).await.unwrap();
//!     client.close().await.unwrap();
//! }
//! ```
//!
//! # Threading model
//!
//! Each client connection is spawned in a separate tokio thread by `tokio::spawn`. A client is
//! represented as a struct and every time a channel is opened the channel struct doesn't spawn any
//! other async thread. When the client is started another thread is created which is controlling
//! the outgoing frames and only them. The client code communicate with that thread with a
//! `tokio::mpsc` channel.
//!
//! So from all of this comes that the next code snippet will block
//!
//! ```no_run
//! use metalmq_client::*;
//!
//! async fn consume_and_publish(channel: &mut Channel) {
//!     let mut handler = channel.basic_consume("consumed-queue", NoAck(false), Exclusive(false),
//!         NoLocal(false)).await.unwrap();
//!     // Get a delivered message as a signal from the queue in the same thread
//!     // This yields as listens the `handler.signal_stream` with a recv
//!     let signal = handler.receive(std::time::Duration::from_secs(1)).await.unwrap();
//!
//!     // This code is not executed, well after the receive above timed out. If we didn't receive
//!     // with timeout this would make a dead-lock.
//!     channel.basic_publish("an-exchange", "", PublishedMessage::default().text("A message"))
//!         .await.unwrap();
//! }
//! ```
//!
//! The correct approach is in those situations is to consume in a different thread. The signal
//! stream is an unbounded stream of `ConsumerSignal` so it won't block.
//!
//! ```no_run
//! use metalmq_client::*;
//! use tokio;
//!
//! async fn consume_and_publish(channel: &mut Channel) {
//!     let mut handler = channel.basic_consume("consumed-queue", NoAck(false), Exclusive(false),
//!         NoLocal(false)).await.unwrap();
//!
//!     let join_handle = tokio::spawn(async move {
//!         let signal = handler.receive(std::time::Duration::from_secs(1)).await.unwrap();
//!
//!         if let ConsumerSignal::Delivered(delivered_message) = signal {
//!             // Process the message here
//!         }
//!     });
//!
//!     channel.basic_publish("an-exchange", "", PublishedMessage::default().text("A message"))
//!         .await.unwrap();
//!
//!     join_handle.await.unwrap();
//! }
//! ```
mod dev;

mod channel_api;
pub use channel_api::{
    Binding, Channel, ExchangeDeclareOpts, ExchangeType, HeaderMatch, IfEmpty, IfUnused, QueueDeclareOpts,
};

mod client_api;
pub use client_api::{Client, EventHandler, EventSignal};

mod consumer;
pub use consumer::{ConsumerHandler, ConsumerSignal, Exclusive, GetHandler, GetSignal, NoAck, NoLocal};

mod error;
pub use error::ClientError;

mod message;
pub use message::{Content, DeliveredMessage, MessageProperties, PublishedMessage, ReturnedMessage};

mod model;
pub use model::{ChannelError, ChannelNumber, ConnectionError};

mod processor;
mod state;
