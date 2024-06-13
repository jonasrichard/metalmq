mod client;
mod config;
mod exchange;
mod message;
mod queue;
mod restapi;

#[cfg(test)]
pub mod tests;

use clap::builder::styling::{AnsiColor, Effects, RgbColor};
use env_logger::Builder;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use log::{error, info};
use std::fmt;
use std::io::Write;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::signal;

pub type Result<T> = std::result::Result<T, Error>;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

#[derive(Clone)]
pub struct Context {
    pub exchange_manager: exchange::manager::ExchangeManagerSink,
    pub queue_manager: queue::manager::QueueManagerSink,
}

#[derive(Debug, Default, PartialEq)]
pub enum ErrorScope {
    #[default]
    Connection,
    Channel,
}

#[derive(Debug, Default)]
pub struct RuntimeError {
    pub scope: ErrorScope,
    pub channel: metalmq_codec::frame::Channel,
    pub code: u16,
    pub text: String,
    pub class_method: u32,
}

impl From<RuntimeError> for metalmq_codec::frame::AMQPFrame {
    fn from(err: RuntimeError) -> metalmq_codec::frame::AMQPFrame {
        match err.scope {
            ErrorScope::Connection => metalmq_codec::frame::connection_close(err.code, &err.text, err.class_method),
            ErrorScope::Channel => {
                metalmq_codec::frame::channel_close(err.channel, err.code, &err.text, err.class_method)
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
            ok @ Ok(_) => ok,
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
            let lvl = buf.default_level_style(record.level());
            lvl.effects(Effects::BOLD);

            match record.level() {
                log::Level::Error => lvl.fg_color(Some(clap::builder::styling::AnsiColor::Red.into())),
                log::Level::Warn => lvl.fg_color(Some(AnsiColor::Yellow.into())),
                log::Level::Info => lvl.fg_color(Some(AnsiColor::Green.into())),
                log::Level::Debug => lvl.fg_color(Some(RgbColor(192, 192, 192).into())),
                log::Level::Trace => lvl.fg_color(Some(RgbColor(96, 96, 96).into())),
            };

            writeln!(
                buf,
                "{} - [{:5}] {}:{} - {}",
                buf.timestamp_millis(),
                record.level(),
                record.file().unwrap_or_default(),
                record.line().unwrap_or_default(),
                record.args()
            )
        })
        .write_style(env_logger::WriteStyle::Always)
        .init();
}

async fn start_http(context: Context, _url: &str) -> Result<()> {
    //let http_addr = url.parse()?;

    //info!("Start HTTP admin API on {}", url);

    let addr: SocketAddr = ([127, 0, 0, 1], 3000).into();
    let listener = TcpListener::bind(addr).await?;

    let ctx = Arc::new(context.clone());

    loop {
        let (tcp, _) = listener.accept().await?;
        let tcp = TokioIo::new(tcp);

        let ctx2 = Arc::clone(&ctx);

        let service = service_fn(move |req| {
            let ctx3 = Arc::clone(&ctx2);

            async move { restapi::route(req, ctx3).await }
        });

        if let Err(err) = http1::Builder::new().serve_connection(tcp, service).await {
            eprintln!("Error http {:?}", err);
        }
    }
}

async fn start_amqp(context: Context, url: &str) -> Result<()> {
    info!("Start AMQP listening on {}", url);

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
        });
    }
}

#[tokio::main]
pub async fn main() -> Result<()> {
    #[cfg(feature = "tracing")]
    console_subscriber::init();

    setup_logger();

    let cli_config = config::cli();

    let config = config::parse_config(&cli_config.config_file_path)?;

    let exchange_manager = exchange::manager::start();
    let context = Context {
        exchange_manager: exchange_manager.clone(),
        queue_manager: queue::manager::start(exchange_manager),
    };

    start_http(context.clone(), &config.network.http_listen).await?;

    start_amqp(context, &config.network.amqp_listen).await?;

    signal::ctrl_c().await?;

    Ok(())
}
