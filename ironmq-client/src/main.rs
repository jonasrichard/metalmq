pub mod client;

use env_logger::Builder;
use log::{info, error};
use std::io::Write;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub type Result<T> = std::result::Result<T, Error>;

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

            client::basic_publish(&connection, 1, "test".into(), "no-key".into(), "Hello, world".into()).await?;

            client::close(&connection).await?
        },
        Err(e) =>
            error!("Error {}", e)
    }

    Ok(())
}
