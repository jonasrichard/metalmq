//! Client of metalmq.
//!
//! # Examples
//!
//! ```no_run
//! use metalmq_client::*;
//!
//! async fn client() -> anyhow::Result<()> {
//!     let mut client = connect("127.0.0.1:5672", "guest", "guest").await?;
//!     client.open("/").await?;
//!     let channel = client.channel_open(1).await?;
//!     channel.basic_publish("exchange", "routing", "Hello".to_string()).await?;
//!     channel.close().await?;
//!     client.close().await?;
//!
//!     Ok(())
//! }
//! ```
pub mod bdd;
mod client;
mod client_sm;

use crate::client::RequestSink;
use anyhow::Result;
use env_logger::Builder;
use metalmq_codec::frame;
use std::fmt;
use std::io::Write;
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
    pub consumer_tag: String,
    pub delivery_tag: u64,
    pub length: usize,
    pub body: Vec<u8>,
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

// TODO implement Drop

/// Represents a connection to AMQP server. It is not a trait since async functions in a trait
/// are not yet supported.
#[derive(Debug)]
pub struct Client {
    sink: RequestSink,
    channels: Vec<Channel>,
}

#[derive(Debug)]
pub struct ClientChannel {
    channel: Channel,
    sink: RequestSink,
}

#[derive(Debug)]
pub struct Consumer {
    channel: Channel,
    sink: RequestSink,
}

/// Connect to an AMQP server.
///
/// This is async code and wait for the [`metalmq_codec::frame::ConnectionTuneOkArgs`] message.
///
/// ```no_run
/// async fn connect() -> anyhow::Result<()> {
///     let conn = metalmq_client::connect("localhost:5672", "guest", "guest").await?;
///     Ok(())
/// }
/// ```
pub async fn connect(url: &str, username: &str, password: &str) -> Result<Client> {
    let sink = client::create_connection(url.to_string()).await?;

    client::sync_call(&sink, frame::AMQPFrame::Header).await?;

    let mut caps = frame::FieldTable::new();

    caps.insert(
        "authentication_failure_close".to_string(),
        frame::AMQPFieldValue::Bool(true),
    );

    //caps.insert("basic.nack".to_string(), AMQPFieldValue::Bool(true));
    //caps.insert("connection.blocked".to_string(), AMQPFieldValue::Bool(true));
    //caps.insert("consumer_cancel_notify".to_string(), AMQPFieldValue::Bool(true));
    //caps.insert("publisher_confirms".to_string(), AMQPFieldValue::Bool(true));

    if client::sync_call(&sink, frame::connection_start_ok(username, password, caps))
        .await
        .is_err()
    {
        return client_error!(
            None,
            503,
            "Server closed connection during authentication",
            frame::CONNECTION_START_OK
        );
    }

    client::call(&sink, frame::connection_tune_ok(0)).await?;

    Ok(Client { sink, channels: vec![] })
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
        client::sync_call(&self.sink, frame::connection_open(0, virtual_host)).await
    }

    /// Close client connection by closing all its channels.
    pub async fn close(&self) -> Result<()> {
        client::sync_call(&self.sink, frame::connection_close(0, 200, "Normal close", 0, 0)).await
    }

    pub async fn channel_open(&mut self, channel: u16) -> Result<ClientChannel> {
        client::sync_call(&self.sink, frame::channel_open(channel)).await?;

        self.channels.push(channel);

        Ok(ClientChannel {
            channel,
            sink: self.sink.clone(),
        })
    }
}

impl ClientChannel {
    pub async fn close(&self) -> Result<()> {
        let (cid, mid) = frame::split_class_method(frame::CHANNEL_CLOSE);

        client::sync_call(
            &self.sink,
            frame::channel_close(self.channel, 200, "Normal close", cid, mid),
        )
        .await
    }

    pub async fn exchange_declare(
        &self,
        exchange_name: &str,
        exchange_type: &str,
        flags: Option<frame::ExchangeDeclareFlags>,
    ) -> Result<()> {
        let frame = frame::exchange_declare(self.channel, exchange_name, exchange_type, flags);

        client::sync_call(&self.sink, frame).await
    }

    pub async fn queue_bind(&self, queue_name: &str, exchange_name: &str, routing_key: &str) -> Result<()> {
        let frame = frame::queue_bind(self.channel, queue_name, exchange_name, routing_key);

        client::sync_call(&self.sink, frame).await
    }

    pub async fn queue_declare(&self, queue_name: &str, flags: Option<frame::QueueDeclareFlags>) -> Result<()> {
        let frame = frame::queue_declare(self.channel, queue_name, flags);

        client::sync_call(&self.sink, frame).await
    }

    pub fn consumer(&self) -> Consumer {
        Consumer {
            channel: self.channel,
            sink: self.sink.clone(),
        }
    }

    pub async fn basic_consume(
        &self,
        queue_name: &str,
        consumer_tag: &str,
        flags: Option<frame::BasicConsumeFlags>,
        sink: MessageSink,
    ) -> Result<()> {
        let frame = frame::basic_consume(self.channel, queue_name, consumer_tag, flags);
        let (tx, rx) = oneshot::channel();

        self.sink
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

    pub async fn basic_publish(&self, exchange_name: &str, routing_key: &str, payload: String) -> Result<()> {
        let frame = frame::basic_publish(self.channel, exchange_name, routing_key);

        self.sink
            .send(client::Request {
                param: client::Param::Publish(frame, payload.as_bytes().to_vec()),
                response: None,
            })
            .await?;

        Ok(())
    }
}

impl Consumer {
    pub async fn basic_ack(&self, delivery_tag: u64) -> Result<()> {
        let frame = frame::basic_ack(self.channel, delivery_tag, false);

        client::call(&self.sink, frame).await
    }
}

/// Convenience function for setting up `env_logger` to see log messages.
pub fn setup_logger() {
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
