mod client;
mod codec;
mod frame;

use env_logger::Builder;
use log::{info, error};
use std::io::Write;

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut builder = Builder::from_default_env();

    builder
        .format_timestamp_millis()
        .format(|buf, record| {
            writeln!(buf, "{} - [{}] :{} {}", buf.timestamp_millis(), record.level(),
                record.line().unwrap_or_default(), record.args())
        })
        .init();

    match client::connect("127.0.0.1:5672".into()).await {
        Ok(connection) => {
            info!("Connection is opened");
            client::open(&connection, "/".into()).await?;
        },
        Err(e) =>
            error!("Error {}", e)
    }

    Ok(())
}
