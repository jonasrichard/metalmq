mod client;
mod exchange;
mod message;
mod queue;
mod restapi;

use env_logger::Builder;
use hyper::service::{make_service_fn, service_fn};
use hyper::Server;
use log::{error, info};
use std::convert::Infallible;
use std::fmt;
use std::io::Write;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio::signal;

pub type Result<T> = std::result::Result<T, Error>;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

#[derive(Clone)]
pub(crate) struct Context {
    pub(crate) exchange_manager: exchange::manager::ExchangeManagerSink,
    pub(crate) queue_manager: queue::manager::QueueManagerSink,
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
    pub(crate) channel: metalmq_codec::frame::Channel,
    pub(crate) code: u16,
    pub(crate) text: String,
    pub(crate) class_id: u16,
    pub(crate) method_id: u16,
}

impl From<RuntimeError> for metalmq_codec::frame::AMQPFrame {
    fn from(err: RuntimeError) -> metalmq_codec::frame::AMQPFrame {
        match err.scope {
            ErrorScope::Connection => {
                metalmq_codec::frame::connection_close(err.channel, err.code, &err.text, err.class_id, err.method_id)
            }
            ErrorScope::Channel => {
                metalmq_codec::frame::channel_close(err.channel, err.code, &err.text, err.class_id, err.method_id)
            }
        }
    }
}

impl std::fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for RuntimeError {}

#[macro_export]
macro_rules! chk {
    ($val:expr) => {
        match $val {
            Ok(ok) => Ok(ok),
            Err(e) => {
                log::error!("Error {:?}", e);

                Err(e)
            }
        }
    };
}

#[macro_export]
macro_rules! logerr {
    ($val:expr) => {
        if let Err(e) = $val {
            error!("Error {:?}", e);
        }
    };
}

#[macro_export]
macro_rules! send {
    ($channel:expr, $message:expr) => {
        $channel
            .send_timeout($message, tokio::time::Duration::from_secs(1))
            .await
    };
}

fn setup_logger() {
    let mut builder = Builder::from_default_env();

    builder
        .format_timestamp_millis()
        .format(|buf, record| {
            let mut lvl = buf.style();
            lvl.set_bold(true);

            match record.level() {
                log::Level::Error => lvl.set_color(env_logger::fmt::Color::Red),
                log::Level::Warn => lvl.set_color(env_logger::fmt::Color::Yellow),
                log::Level::Info => lvl.set_color(env_logger::fmt::Color::Green),
                log::Level::Debug => lvl.set_color(env_logger::fmt::Color::Rgb(160, 160, 160)),
                log::Level::Trace => lvl.set_color(env_logger::fmt::Color::Rgb(96, 96, 96)),
            };

            writeln!(
                buf,
                "{} - [{:5}] {}:{} - {}",
                buf.timestamp_millis(),
                lvl.value(record.level()),
                record.file().unwrap_or_default(),
                record.line().unwrap_or_default(),
                record.args()
            )
        })
        .write_style(env_logger::WriteStyle::Always)
        .init();
}

async fn start_http(context: Context) -> Result<()> {
    let http_addr = SocketAddr::from(([127, 0, 0, 1], 3000));

    info!("Start HTTP admin API on port 3000");

    let make_svc = make_service_fn(move |_conn| {
        let context = context.clone();
        async move { Ok::<_, Infallible>(service_fn(move |req| restapi::route(req, context.clone()))) }
    });

    let server = Server::bind(&http_addr).serve(make_svc);

    tokio::spawn(async move {
        if let Err(e) = server.await {
            eprintln!("HTTP error {}", e);
        }
    });

    Ok(())
}

async fn start_amqp(context: Context, url: &str) -> Result<()> {
    info!("Listening on port 5672");

    let listener = TcpListener::bind(url).await?;

    loop {
        let (socket, _) = listener.accept().await?;
        let ctx = Context {
            queue_manager: context.queue_manager.clone(),
            exchange_manager: context.exchange_manager.clone(),
        };

        tokio::spawn(async move {
            if let Err(e) = client::conn::handle_client(socket, ctx).await {
                error!("Error handling client {:?}", e)
            }

            info!("Connection is closed");
        });
    }
}

#[tokio::main]
pub async fn main() -> Result<()> {
    setup_logger();

    let context = Context {
        exchange_manager: exchange::manager::start(),
        queue_manager: queue::manager::start(),
    };

    start_http(context.clone()).await?;

    start_amqp(context, "[::1]:5672").await?;

    signal::ctrl_c().await?;

    Ok(())
}
