mod client;
mod codec;
mod frame;

use env_logger::Builder;
use std::io::Write;
//use tokio::prelude::*;

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

    // TODO use client here
    client::connect("127.0.0.1:5672".into());

    Ok(())
}
