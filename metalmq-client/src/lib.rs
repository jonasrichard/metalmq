//! Client of metalmq.
//!
//! # Examples
//!
//! ```no_run
//! use metalmq_client::*;
//!
//! async fn client() -> Result<()> {
//!     let client = connect("127.0.0.1:5672").await?;
//!     client.open("/").await?;
//!     client.channel_open(1).await?;
//!     client.basic_publish(1, "exchange", "routing", "Hello".into()).await?;
//!     client.close().await?;
//!
//!     Ok(())
//! }
//! ```
pub mod bdd;
mod client;
mod client_sm;

use anyhow::Result;
use log4rs::append::console::ConsoleAppender;
use log4rs::append::file::FileAppender;
use log4rs::config::{Appender, Config, Logger, Root};
use log4rs::encode::pattern::PatternEncoder;
use metalmq_codec::frame;
use std::fmt;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot};

/// AMQP channel number
pub type Channel = frame::Channel;
/// AMQP method class id
pub type ClassId = frame::ClassId;
/// AMQP class id method id number
pub type ClassMethod = frame::ClassMethod;

// TODO feature log should log in trace level

/// Interface for consuming messages.
pub type MessageSink = mpsc::Sender<Message>;

/// Message type for consuming messages.
#[derive(Debug)]
pub struct Message {
    pub channel: Channel,
    pub body: Vec<u8>,
    pub length: usize,
}

/// Represents a connection or channel error. If `channel` is `None` it is a
/// connection error.
#[derive(Clone, Debug)]
pub struct ClientError {
    pub channel: Option<Channel>,
    pub code: u16,
    pub message: String,
    pub class_method: ClassMethod,
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for ClientError {}

/// Shorthand for creating errors in async functions.
#[macro_export]
macro_rules! client_error {
    ($channel:expr, $code:expr, $message:expr, $cm:expr) => {
        ::std::result::Result::Err(anyhow::Error::new($crate::ClientError {
            channel: $channel,
            code: $code,
            message: ::std::string::String::from($message),
            class_method: $cm,
        }))
    };
}

/// Represents a connection to AMQP server. It is not a trait since async functions in a trait
/// are not yet supported.
pub struct Client {
    server_channel: mpsc::Sender<client::Request>,
}

pub struct ClientChannel {
    server_channel: mpsc::Sender<client::Request>,
}

/// Connect to an AMQP server.
///
/// This is async code and wait for the [`metalmq_codec::frame::ConnectionTuneOkArgs`] message.
///
/// ```no_run
/// async fn connect() -> metalmq_client::Result<()> {
///     let conn = metalmq_client::connect("127.0.0.1:5672", "guest", "guest").await?;
///     Ok(())
/// }
/// ```
pub async fn connect(url: &str, username: &str, password: &str) -> Result<Client> {
    let connection = client::create_connection(url.into()).await?;

    client::sync_call(&connection, frame::AMQPFrame::Header).await?;

    let mut caps = frame::FieldTable::new();

    caps.insert("authentication_failure_close".into(), frame::AMQPFieldValue::Bool(true));

    //caps.insert("basic.nack".into(), AMQPFieldValue::Bool(true));
    //caps.insert("connection.blocked".into(), AMQPFieldValue::Bool(true));
    //caps.insert("consumer_cancel_notify".into(), AMQPFieldValue::Bool(true));
    //caps.insert("pub(crate)lisher_confirms".into(), AMQPFieldValue::Bool(true));

    if let Err(_) = client::sync_call(&connection, frame::connection_start_ok(username, password, caps)).await {
        return client_error!(
            None,
            503,
            "Server closed connection during authentication",
            frame::CONNECTION_START_OK
        );
    }

    client::call(&connection, frame::connection_tune_ok(0)).await?;

    Ok(connection)
}

impl Client {
    /// Client "connects" to a virtual host. The virtual host may or may not exist,
    /// in case of an error we got a `ClientError` and the connection closes.
    ///
    /// ```no_run
    /// use metalmq_client::*;
    ///
    /// async fn vhost(c: &Client) {
    ///     if let Err(ce) = c.open("/invalid").await {
    ///         eprintln!("Virtual host does not exist");
    ///     }
    /// }
    /// ```
    pub async fn open(&self, virtual_host: &str) -> Result<()> {
        client::sync_call(&self, frame::connection_open(0, virtual_host.into())).await
    }

    pub async fn close(&self) -> Result<()> {
        client::sync_call(&self, frame::connection_close(0, 200, "Normal close", 0, 0)).await
    }

    pub async fn channel_open(&self, channel: u16) -> Result<ClientChannel> {
        client::sync_call(&self, frame::channel_open(channel)).await?;

        Ok(ClientChannel {
            server_channel: self.server_channel.clone(),
        })
    }

    pub async fn channel_close(&self, channel: Channel) -> Result<()> {
        let (cid, mid) = frame::split_class_method(frame::CHANNEL_CLOSE);

        client::sync_call(&self, frame::channel_close(channel, 200, "Normal close", cid, mid)).await
    }

    pub async fn exchange_declare(
        &self,
        channel: Channel,
        exchange_name: &str,
        exchange_type: &str,
        flags: Option<frame::ExchangeDeclareFlags>,
    ) -> Result<()> {
        let frame = frame::exchange_declare(channel, exchange_name.into(), exchange_type.into(), flags);

        client::sync_call(&self, frame).await
    }

    pub async fn queue_bind(
        &self,
        channel: u16,
        queue_name: &str,
        exchange_name: &str,
        routing_key: &str,
    ) -> Result<()> {
        let frame = frame::queue_bind(channel, queue_name.into(), exchange_name.into(), routing_key.into());

        client::sync_call(&self, frame).await
    }

    pub async fn queue_declare(&self, channel: Channel, queue_name: &str) -> Result<()> {
        let frame = frame::queue_declare(channel, queue_name.into());

        client::sync_call(&self, frame).await
    }

    // TODO make a channel struct and put channel specific operations there
    pub async fn basic_ack(&self, channel: Channel, delivery_tag: u64) -> Result<()> {
        let frame = frame::basic_ack(channel, delivery_tag, false);

        client::call(&self, frame).await
    }

    pub async fn basic_consume(
        &self,
        channel: Channel,
        queue_name: &str,
        consumer_tag: &str,
        sink: MessageSink,
    ) -> Result<()> {
        let frame = frame::basic_consume(channel, queue_name.into(), &consumer_tag.to_string());
        let (tx, rx) = oneshot::channel();

        self.server_channel
            .send(client::Request {
                param: client::Param::Consume(frame, sink),
                response: Some(tx),
            })
            .await?;

        match rx.await {
            Ok(response) => match response {
                Ok(()) => Ok(()),
                Err(e) => Err(e),
            },
            Err(_) => client_error!(None, 501, "Channel recv error", 0),
        }
    }

    pub async fn basic_publish(
        &self,
        channel: Channel,
        exchange_name: &str,
        routing_key: &str,
        payload: String,
    ) -> Result<()> {
        let frame = frame::basic_publish(channel, exchange_name.into(), routing_key.into());

        self.server_channel
            .send(client::Request {
                param: client::Param::Publish(frame, payload.as_bytes().to_vec()),
                response: None,
            })
            .await?;

        Ok(())
    }
}

/// Convenience function for setting up `env_logger` to see log messages.
pub fn setup_logger() {
    let stdout = ConsoleAppender::builder().build();

    let clientlog = FileAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{d} - {m}{n}")))
        .build("client.log")
        .unwrap();

    let config = Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .appender(Appender::builder().build("clientlog", Box::new(clientlog)))
        .logger(
            Logger::builder()
                .appender("clientlog")
                .additive(false)
                .build("client", log::LevelFilter::Info),
        )
        .build(Root::builder().appender("stdout").build(log::LevelFilter::Debug))
        .unwrap();

    log4rs::init_config(config).unwrap();

    //let mut builder = Builder::from_default_env();

    //builder
    //    .format_timestamp_millis()
    //    .format(|buf, record| {
    //        writeln!(
    //            buf,
    //            "{} - [{}] {}:{} {}",
    //            buf.timestamp_millis(),
    //            record.level(),
    //            record.file().unwrap_or_default(),
    //            record.line().unwrap_or_default(),
    //            record.args()
    //        )
    //    })
    //    .init();
}

#[allow(dead_code)]
async fn publish_bench(client: &Client) -> Result<()> {
    let now = Instant::now();
    let mut total = 0u32;

    for _ in 0..100_000u32 {
        client
            .basic_publish(1, "test".into(), "no-key".into(), "Hello, world".into())
            .await?;
        total += 1;
    }

    println!("{}/100,000 publish takes {} us", total, now.elapsed().as_micros());

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn send_client_error() {
        let (tx, rx) = tokio::sync::oneshot::channel::<Result<()>>();

        tx.send(client_error!(None, 404, "Not found", 0)).unwrap();

        let res = rx.await.unwrap();

        assert!(res.is_err());

        let err = res.unwrap_err().downcast::<ClientError>().unwrap();
        assert_eq!(err.channel, None);
        assert_eq!(err.code, 404);
        assert_eq!(err.message, "Not found".to_string());
        assert_eq!(err.class_method, 0);
    }
}
