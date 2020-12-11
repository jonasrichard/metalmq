//! Client of ironmq.
//!
//! Usage
//! ```no_run
//! use ironmq_client::client;
//!
//! async fn client() -> ironmq_client::Result<()> {
//!     let conn = client::connect("127.0.0.1:5672").await?;
//!     client::open(&conn, "/".into()).await?;
//!     client::channel_open(&conn, 1).await?;
//!     client::basic_publish(&conn, 1, "exchange", "routing", "Hello".into()).await?;
//!     client::close(&conn).await?;
//!
//!     Ok(())
//! }
//! ```
pub mod client;
mod client_sm;

use env_logger::Builder;
use log::{info, error};
use std::fmt;
use std::io::Write;
use std::time::Instant;

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

//type ConsumeHandler = fn(String) -> bool;
type ConsumeCallback = Box<dyn Fn(String) -> String + Send + Sync>;

#[allow(dead_code)]
async fn publish_bench(connection: &client::Connection) -> Result<()> {
    let now = Instant::now();
    let mut total = 0u32;

    for _ in 0..100_000u32 {
        client::basic_publish(&connection, 1, "test".into(), "no-key".into(), "Hello, world".into()).await?;
        total += 1;
    }

    println!("{}/100,000 publish takes {} us", total, now.elapsed().as_micros());

    Ok(())
}

fn consumer_handler(s: String) -> String {
    info!("Handling content {}", s);
    "".into()
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let mut builder = Builder::from_default_env();

    builder
        .format_timestamp_millis()
        .format(|buf, record| {
            writeln!(buf, "{} - [{}] {}:{} {}", buf.timestamp_millis(), record.level(),
                record.file().unwrap_or_default(), record.line().unwrap_or_default(), record.args())
        })
        .init();

    let exchange = "test";
    let queue = "queue-test";
    let consumer_tag = "ctag1";

    match client::connect("127.0.0.1:5672".into()).await {
        Ok(connection) => {
            info!("Connection is opened");
            client::open(&connection, "/".into()).await?;
            client::channel_open(&connection, 1).await?;

            client::exchange_declare(&connection, 1, exchange, "fanout").await?;
            client::queue_declare(&connection, 1, queue).await?;
            client::queue_bind(&connection, 1, queue, exchange, "").await?;

            client::basic_publish(&connection, 1, exchange, "no-key", "Hey man".into()).await?;

            client::basic_consume(&connection, 1, queue, consumer_tag, consumer_handler).await?;

            let (_tx, rx) = tokio::sync::oneshot::channel::<()>();
            if let Err(e) = rx.await {
                error!("Error {}", e)
            }

            client::close(&connection).await?
        },
        Err(e) =>
            error!("Error {}", e)
    }

    Ok(())
}
