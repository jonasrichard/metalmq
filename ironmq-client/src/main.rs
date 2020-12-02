pub mod client;
mod client_sm;

use env_logger::Builder;
use log::{info, error};
use std::fmt;
use std::io::Write;
use std::time::Instant;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub type Result<T> = std::result::Result<T, Error>;

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
        Err(Box::new(crate::ClientError {
            code: $code,
            message: String::from($message)
        }))
    }
}

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

    match client::connect("127.0.0.1:5672".into()).await {
        Ok(connection) => {
            info!("Connection is opened");
            client::open(&connection, "/".into()).await?;
            client::channel_open(&connection, 1).await?;

            client::exchange_declare(&connection, 1, "test", "fanout").await?;
            client::queue_declare(&connection, 1, "queue-test").await?;
            client::queue_bind(&connection, 1, "queue-test", "test", "").await?;

            client::close(&connection).await?
        },
        Err(e) =>
            error!("Error {}", e)
    }

    Ok(())
}
