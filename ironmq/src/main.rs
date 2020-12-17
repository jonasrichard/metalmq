mod client;
mod exchange;
mod message;
mod queue;

use env_logger::Builder;
use log::{error, info};
use std::fmt;
use std::io::Write;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

pub type Result<T> = std::result::Result<T, Error>;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub(crate) struct Context {
    pub(crate) exchanges: exchange::Exchanges,
}

#[derive(Debug, PartialEq)]
pub(crate) enum ErrorScope {
    Connection,
    Channel,
}

impl Default for ErrorScope {
    fn default() -> Self {
        ErrorScope::Connection
    }
}

#[derive(Debug, Default)]
pub(crate) struct RuntimeError {
    pub(crate) scope: ErrorScope,
    pub(crate) channel: ironmq_codec::frame::Channel,
    pub(crate) code: u16,
    pub(crate) text: String,
    pub(crate) class_id: u16,
    pub(crate) method_id: u16,
}

impl From<RuntimeError> for ironmq_codec::frame::AMQPFrame {
    fn from(err: RuntimeError) -> ironmq_codec::frame::AMQPFrame {
        match err.scope {
            ErrorScope::Connection => ironmq_codec::frame::connection_close(
                err.channel,
                err.code,
                &err.text,
                err.class_id,
                err.method_id,
            ),
            ErrorScope::Channel => ironmq_codec::frame::channel_close(
                err.channel,
                err.code,
                &err.text,
                err.class_id,
                err.method_id,
            ),
        }
    }
}

impl std::fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for RuntimeError {}

fn setup_logger() {
    let mut builder = Builder::from_default_env();

    builder
        .format_timestamp_millis()
        .format(|buf, record| {
            writeln!(
                buf,
                "{} - [{}] {}:{} {}",
                buf.timestamp_millis(),
                record.level(),
                record.file().unwrap_or_default(),
                record.line().unwrap_or_default(),
                record.args()
            )
        })
        .init();
}

#[tokio::main]
pub async fn main() -> Result<()> {
    setup_logger();

    let exchanges = exchange::start();

    let context = Arc::new(Mutex::new(Context {
        exchanges: exchanges,
    }));

    info!("Listening on port 5672");

    let listener = TcpListener::bind("127.0.0.1:5672").await?;

    loop {
        let (socket, _) = listener.accept().await?;
        let ctx = context.clone();

        tokio::spawn(async move {
            if let Err(e) = client::conn::handle_client(socket, ctx).await {
                error!("Error handling client {:?}", e)
            }

            info!("Connection is closed");
        });
    }
}
