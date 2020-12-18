//! Client of ironmq.
//!
//! Usage
//! ```no_run
//! use ironmq_client::*;
//!
//! async fn client() -> Result<()> {
//!     let conn = connect("127.0.0.1:5672".to_string()).await?;
//!     open(&conn, "/".into()).await?;
//!     channel_open(&conn, 1).await?;
//!     basic_publish(&conn, 1, "exchange", "routing", "Hello".into()).await?;
//!     close(&conn).await?;
//!
//!     Ok(())
//! }
//! ```
pub mod client;
mod client_sm;

use env_logger::Builder;
use ironmq_codec::frame;
use std::collections::HashMap;
use std::fmt;
use std::io::Write;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot};

/// AMQP channel number
pub type Channel = frame::Channel;
/// AMQP method class id
pub type ClassId = frame::ClassId;
/// AMQP class id method id number
pub type ClassMethod = frame::ClassMethod;

// TODO feature log should log in trace level

/// Custom error type which uses `Error` as an error type.
pub type Result<T> = std::result::Result<T, Error>;

/// A sendable, syncable boxed error, usable between async threads.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

#[derive(Clone, Debug)]
pub struct ClientError {
    pub channel: Option<Channel>,
    pub code: u16,
    pub message: String,
    pub class_method: u32
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for ClientError {
}

#[macro_export]
macro_rules! client_error {
    ($channel:expr, $code:expr, $message:expr, $cm:expr) => {
        ::std::result::Result::Err(::std::boxed::Box::new($crate::ClientError {
            channel: $channel,
            code: $code,
            message: ::std::string::String::from($message),
            class_method: $cm
        }))
    }
}

type ConsumeCallback = Box<dyn Fn(String) -> String + Send + Sync>;

/// Represents a connection to AMQP server. It is not a trait since async functions in a trait
/// are not yet supported.
pub struct Connection {
    server_channel: mpsc::Sender<client::Request>,
}

/// Connect to an AMQP server.
///
/// This is async code and wait for the Connection.Tune-Ok message.
///
/// ```no_run
/// async fn connect() -> ironmq_client::Result<()> {
///     let url = "127.0.0.1:5672".to_string();
///
///     let conn = ironmq_client::connect(url).await?;
///     Ok(())
/// }
/// ```
pub async fn connect(url: String) -> Result<Box<Connection>> {
    let connection = client::create_connection(url).await?;

    client::sync_call(&connection, frame::AMQPFrame::Header).await?;
    client::sync_call(&connection, frame::connection_start_ok("guest", "guest", HashMap::new()),) .await?;
    client::call(&connection, frame::connection_tune_ok(0)).await?;

    Ok(connection)
}

/// Client "connects" to a virtual host. The virtual host may or may not exist,
/// in case of an error we got a `ClientError` and the connection closes.
///
/// ```no_run
/// use ironmq_client::*;
///
/// async fn open(c: &Connection) {
///     if let ironmq_client::Error(ce) = open(&c, "/invalid".to_string()) {
///         eprintln!("Virtual host does not exist");
///     }
/// }
/// ```
pub async fn open(connection: &Connection, virtual_host: String) -> Result<()> {
    client::sync_call(&connection, frame::connection_open(0, virtual_host)).await?;

    Ok(())
}

pub async fn close(connection: &Connection) -> Result<()> {
    client::sync_call(&connection, frame::connection_close(0, 200, "Normal close", 0, 0)).await?;

    Ok(())
}

pub async fn channel_open(connection: &Connection, channel: u16) -> Result<()> {
    client::sync_call(&connection, frame::channel_open(channel)).await?;

    Ok(())
}

pub async fn channel_close(connection: &Connection, channel: Channel) -> Result<()> {
    let (cid, mid) = frame::split_class_method(frame::CHANNEL_CLOSE);

    client::sync_call(&connection, frame::channel_close(channel, 200, "Normal close", cid, mid)).await?;

    Ok(())
}

pub async fn exchange_declare(connection: &Connection, channel: u16, exchange_name: &str,
                              exchange_type: &str, flags: Option<frame::ExchangeDeclareFlags>) -> Result<()> {
    let frame = frame::exchange_declare(channel, exchange_name.into(), exchange_type.into(), flags);

    client::sync_call(&connection, frame).await?;

    Ok(())
}

pub async fn queue_bind(connection: &Connection, channel: u16, queue_name: &str, exchange_name: &str,
                        routing_key: &str) -> Result<()> {
    let frame = frame::queue_bind(channel, queue_name.into(), exchange_name.into(), routing_key.into());

    client::sync_call(&connection, frame).await?;

    Ok(())
}

pub async fn queue_declare(connection: &Connection, channel: u16, queue_name: &str) -> Result<()> {
    let frame = frame::queue_declare(channel, queue_name.into());

    client::sync_call(&connection, frame).await?;

    Ok(())
}

pub async fn basic_consume<'a>(connection: &Connection, channel: u16, queue_name: &'a str, consumer_tag: &'a str,
                               cb: fn(String) -> String) -> Result<()> {
    let frame = frame::basic_consume(channel, queue_name.into(), consumer_tag.into());
    let (tx, rx) = oneshot::channel();

    connection.server_channel.send(client::Request {
        param: client::Param::Consume(frame, Box::new(cb)),
        response: Some(tx)
    }).await?;

    match rx.await {
        Ok(response) => match response {
            Ok(()) => Ok(()),
            Err(e) => Err(e)
        },
        Err(_) => client_error!(None, 501, "Channel recv error", 0)
    }
}

pub async fn basic_publish(connection: &Connection, channel: u16, exchange_name: &str, routing_key: &str,
                           payload: String) -> Result<()> {
    let frame = frame::basic_publish(channel, exchange_name.into(), routing_key.into());

    connection.server_channel.send(client::Request {
        param: client::Param::Publish(frame, payload.as_bytes().to_vec()),
        response: None
    }).await?;

    Ok(())
}

/// Convenience function for setting up `env_logger` to see log messages.
pub fn setup_logger() {
    let mut builder = Builder::from_default_env();

    builder
        .format_timestamp_millis()
        .format(|buf, record| {
            writeln!(buf, "{} - [{}] {}:{} {}", buf.timestamp_millis(), record.level(),
                record.file().unwrap_or_default(), record.line().unwrap_or_default(), record.args()
            )
        }).init();
}

#[allow(dead_code)]
async fn publish_bench(connection: &Connection) -> Result<()> {
    let now = Instant::now();
    let mut total = 0u32;

    for _ in 0..100_000u32 {
        basic_publish(&connection, 1, "test".into(), "no-key".into(), "Hello, world".into()).await?;
        total += 1;
    }

    println!("{}/100,000 publish takes {} us", total, now.elapsed().as_micros());

    Ok(())
}
