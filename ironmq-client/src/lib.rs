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

use frame::Channel;
use ironmq_codec::frame;
use std::collections::HashMap;
use std::fmt;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot};

// TODO expose Channel type and put it here!

// TODO feature log should log in trace level

pub type Result<T> = std::result::Result<T, Error>;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub struct ClientError {
    pub code: u16,
    pub message: String
}

impl std::fmt::Debug for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientError")
         .field("code", &self.code)
         .field("message", &self.message)
         .finish()
    }
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientError")
         .field("code", &self.code)
         .field("message", &self.message)
         .finish()
    }
}

impl std::error::Error for ClientError {
}

#[macro_export]
macro_rules! client_error {
    ($code:expr, $message:expr) => {
        ::std::result::Result::Err(::std::boxed::Box::new($crate::ClientError {
            code: $code,
            message: ::std::string::String::from($message)
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
///     let conn = ironmq_client::connect("127.0.0.1:5672".to_string()).await?;
///     Ok(())
/// }
/// ```
pub async fn connect(url: String) -> Result<Box<Connection>> {
    let connection = client::create_connection(url).await?;

    client::sync_call(&connection, frame::AMQPFrame::Header).await?;
    client::sync_call(
        &connection,
        frame::connection_start_ok("guest", "guest", HashMap::new()),
    )
    .await?;
    client::call(&connection, frame::connection_tune_ok(0)).await?;

    Ok(connection)
}

pub async fn open(connection: &Connection, virtual_host: String) -> Result<()> {
    client::sync_call(&connection, frame::connection_open(0, virtual_host)).await?;

    Ok(())
}

pub async fn close(connection: &Connection) -> Result<()> {
    client::sync_call(&connection, frame::connection_close(0)).await?;

    Ok(())
}

pub async fn channel_open(connection: &Connection, channel: u16) -> Result<()> {
    client::sync_call(&connection, frame::channel_open(channel)).await?;

    Ok(())
}

pub async fn channel_close(connection: &Connection, channel: Channel) -> Result<()> {
    let (cid, mid) = frame::split_class_method(frame::CHANNEL_CLOSE);

    client::sync_call(
        &connection,
        frame::channel_close(channel, 200, "Normal close", cid, mid)
    ).await?;

    Ok(())
}

pub async fn exchange_declare(
    connection: &Connection,
    channel: u16,
    exchange_name: &str,
    exchange_type: &str,
) -> Result<()> {
    let frame = frame::exchange_declare(channel, exchange_name.into(), exchange_type.into());

    client::sync_call(&connection, frame).await?;

    Ok(())
}

pub async fn queue_bind(
    connection: &Connection,
    channel: u16,
    queue_name: &str,
    exchange_name: &str,
    routing_key: &str,
) -> Result<()> {
    let frame = frame::queue_bind(channel, queue_name.into(), exchange_name.into(), routing_key.into());

    client::sync_call(&connection, frame).await?;

    Ok(())
}

pub async fn queue_declare(connection: &Connection, channel: u16, queue_name: &str) -> Result<()> {
    let frame = frame::queue_declare(channel, queue_name.into());

    client::sync_call(&connection, frame).await?;

    Ok(())
}

pub async fn basic_consume<'a>(
    connection: &Connection,
    channel: u16,
    queue_name: &'a str,
    consumer_tag: &'a str,
    cb: fn(String) -> String,
) -> Result<()> {
    let frame = frame::basic_consume(channel, queue_name.into(), consumer_tag.into());
    let (tx, rx) = oneshot::channel();

    connection.server_channel.send(client::Request {
        param: client::Param::Consume(frame, Box::new(cb)),
        response: Some(tx)
    }).await?;

    match rx.await {
        Ok(()) => Ok(()),
        Err(_) => client_error!(0, "Channel recv error"),
    }
}

pub async fn basic_publish(
    connection: &Connection,
    channel: u16,
    exchange_name: &str,
    routing_key: &str,
    payload: String,
) -> Result<()> {
    let frame = frame::basic_publish(channel, exchange_name.into(), routing_key.into());

    connection.server_channel.send(client::Request {
        param: client::Param::Publish(frame, payload.as_bytes().to_vec()),
        response: None
    }).await?;

    Ok(())
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

//fn consumer_handler(s: String) -> String {
//    info!("Handling content {}", s);
//    "".into()
//}
//
//#[tokio::main]
//pub async fn main() -> Result<()> {
//    let mut builder = Builder::from_default_env();
//
//    builder
//        .format_timestamp_millis()
//        .format(|buf, record| {
//            writeln!(buf, "{} - [{}] {}:{} {}", buf.timestamp_millis(), record.level(),
//                record.file().unwrap_or_default(), record.line().unwrap_or_default(), record.args())
//        })
//        .init();
//
//    let exchange = "test";
//    let queue = "queue-test";
//    let consumer_tag = "ctag1";
//
//    match connect("127.0.0.1:5672".into()).await {
//        Ok(connection) => {
//            info!("Connection is opened");
//            open(&connection, "/".into()).await?;
//            channel_open(&connection, 1).await?;
//
//            exchange_declare(&connection, 1, exchange, "fanout").await?;
//            queue_declare(&connection, 1, queue).await?;
//            queue_bind(&connection, 1, queue, exchange, "").await?;
//
//            basic_publish(&connection, 1, exchange, "no-key", "Hey man".into()).await?;
//
//            basic_consume(&connection, 1, queue, consumer_tag, consumer_handler).await?;
//
//            let (_tx, rx) = tokio::sync::oneshot::channel::<()>();
//            if let Err(e) = rx.await {
//                error!("Error {}", e)
//            }
//
//            close(&connection).await?
//        },
//        Err(e) =>
//            error!("Error {}", e)
//    }
//
//    Ok(())
//}
