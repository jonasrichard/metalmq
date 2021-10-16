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
mod client;
mod client_sm;

use crate::client::RequestSink;
use anyhow::Result;
use env_logger::Builder;
use metalmq_codec::frame;
use std::fmt;
use std::future::Future;
use std::io::Write;
use std::pin::Pin;
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
pub struct Message {
    pub channel: Channel,
    pub consumer_tag: String,
    pub delivery_tag: u64,
    pub length: usize,
    pub body: Vec<u8>,
}

impl std::fmt::Debug for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let body = String::from_utf8_lossy(&self.body[..std::cmp::min(64usize, self.body.len())]);

        f.write_fmt(format_args!(
            "Message {{ channel: {}, consumer_tag: {}, delivery_tag: {}, body: \"{}\" }}",
            &self.channel, &self.consumer_tag, &self.delivery_tag, body
        ))
    }
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

pub enum ConsumeInput {
    Delivered(Message),
    Cancelled,
    Error,
}

pub enum ConsumeResponse {
    Ack {
        delivery_tag: u64,
        multiple: bool,
    },
    Nack {
        delivery_tag: u64,
        multiple: bool,
        requeue: bool,
    },
    Reject {
        delivery_tag: u64,
        requeue: bool,
    },
    Nothing,
}

pub struct ConsumeResult<T> {
    pub result: Option<T>,
    pub ack_response: ConsumeResponse,
}

pub type ConsumerFn<T> = dyn FnMut(ConsumeInput) -> ConsumeResult<T> + Send + Sync;

pub type ReturnCallback = Box<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

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

    /// Opens a channel an gives back the channel handler.
    ///
    /// All the major operations can be done through the channel.
    ///
    /// ```no_run
    /// use metalmq_client::*;
    ///
    /// async fn publish(c: &mut Client) -> anyhow::Result<()> {
    ///     let ch = c.channel_open(3).await?;
    ///
    ///     ch.basic_publish("exchange-name", "", "Here is the payload".to_string()).await?;
    ///     ch.close().await?;
    ///
    ///     Ok(())
    /// }
    /// ```
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
    /// Closes the channel.
    pub async fn close(&self) -> Result<()> {
        let (cid, mid) = frame::split_class_method(frame::CHANNEL_CLOSE);

        client::sync_call(
            &self.sink,
            frame::channel_close(self.channel, 200, "Normal close", cid, mid),
        )
        .await
    }

    /// Declare exchange.
    pub async fn exchange_declare(
        &self,
        exchange_name: &str,
        exchange_type: &str,
        flags: Option<frame::ExchangeDeclareFlags>,
    ) -> Result<()> {
        let frame = frame::exchange_declare(self.channel, exchange_name, exchange_type, flags);

        client::sync_call(&self.sink, frame).await
    }

    /// Delete exchange.
    pub async fn exchange_delete(&self, exchange_name: &str, if_unused: bool) -> Result<()> {
        let mut flags = frame::ExchangeDeleteFlags::default();

        if if_unused {
            flags.toggle(frame::ExchangeDeleteFlags::IF_UNUSED);
        }

        let frame = frame::exchange_delete(self.channel, exchange_name, Some(flags));

        client::sync_call(&self.sink, frame).await
    }

    /// Declare queue.
    pub async fn queue_declare(&self, queue_name: &str, flags: Option<frame::QueueDeclareFlags>) -> Result<()> {
        let frame = frame::queue_declare(self.channel, queue_name, flags);

        client::sync_call(&self.sink, frame).await
    }

    /// Bind queue to exchange.
    pub async fn queue_bind(&self, queue_name: &str, exchange_name: &str, routing_key: &str) -> Result<()> {
        let frame = frame::queue_bind(self.channel, queue_name, exchange_name, routing_key);

        client::sync_call(&self.sink, frame).await
    }

    pub async fn queue_unbind(&self, queue_name: &str, exchange_name: &str, routing_key: &str) -> Result<()> {
        let frame = frame::queue_unbind(self.channel, queue_name, exchange_name, routing_key);

        client::sync_call(&self.sink, frame).await
    }

    pub async fn queue_purge(&self, queue_name: &str) -> Result<()> {
        Ok(())
    }

    pub async fn queue_delete(&self, queue_name: &str, if_unused: bool, if_empty: bool) -> Result<()> {
        let mut flags = frame::QueueDeleteFlags::empty();
        flags.set(frame::QueueDeleteFlags::IF_UNUSED, if_unused);
        flags.set(frame::QueueDeleteFlags::IF_EMPTY, if_empty);

        let frame = frame::queue_delete(self.channel, queue_name, Some(flags));

        client::sync_call(&self.sink, frame).await
    }

    // TODO consume should spawn a thread and on that thread the client can
    // execute its callback. From that thread we can ack or reject the message,
    // so the consumer channel won't be affected. Also in the consumer channel,
    // the client.rs module can buffer the messages, so if the server support
    // some kind of qos, it won't send more messages while the client has a
    // lot of unacked messages.
    //
    // Because of the lifetimes it would be nice if we consume on a channel, we
    // give up the ownership and move the channel inside the tokio thread. Why?
    // Because inside the thread on the channel we need to send back acks or
    // nacks and so on, so the thread uses the channel. But since we don't want
    // to run into multithreading issue, we need to move the channel to the
    // thread and forget that channel in the main code which consumes.

    pub async fn basic_consume<'a, T: Send + 'static>(
        &'a self,
        queue_name: &'a str,
        consumer_tag: &'a str,
        flags: Option<frame::BasicConsumeFlags>,
        mut consumer: Box<ConsumerFn<T>>,
    ) -> Result<oneshot::Receiver<Option<T>>> {
        let frame = frame::basic_consume(self.channel, queue_name, consumer_tag, flags);
        let (tx, rx) = oneshot::channel();

        // TODO this will be the buffer of the inflight messages
        let (sink, mut stream) = mpsc::channel::<Message>(16);

        // Clone the channel in order that users can use this ClientChannel
        // to publish messages.
        let channel_clone = ClientChannel {
            channel: self.channel,
            sink: self.sink.clone(),
        };

        let (consume_tx, consume_rx) = oneshot::channel();

        tokio::spawn(async move {
            // Result of the oneshot channel what the user listens for the result of the
            // whole consume process.
            let mut final_result: Option<T> = None;

            loop {
                match stream.recv().await {
                    Some(message) => {
                        match consumer(ConsumeInput::Delivered(message)) {
                            ConsumeResult { result, ack_response } => {
                                match ack_response {
                                    ConsumeResponse::Ack { delivery_tag, multiple } => {
                                        channel_clone.basic_ack(delivery_tag, multiple).await;
                                        ()
                                    }
                                    _ => unimplemented!(),
                                }

                                match result {
                                    r @ Some(_) => {
                                        final_result = r;
                                        break;
                                    }
                                    _ => (),
                                }
                            }
                        };
                    }
                    None => match consumer(ConsumeInput::Cancelled) {
                        _ => {
                            break;
                        }
                    },
                }
            }

            consume_tx.send(final_result);
        });

        self.sink
            .send(client::Request {
                param: client::Param::Consume(frame, sink),
                response: Some(tx),
            })
            .await?;

        match rx.await {
            Ok(response) => match response {
                Ok(()) => Ok(consume_rx),
                Err(e) => Err(e),
            },
            Err(_) => client_error!(None, 501, "Channel recv error", 0),
        }
    }

    pub async fn basic_cancel(&self, consumer_tag: &str) -> Result<()> {
        let frame = frame::basic_cancel(self.channel, consumer_tag, false);
        let (tx, rx) = oneshot::channel();

        self.sink
            .send(client::Request {
                param: client::Param::Frame(frame),
                response: Some(tx),
            })
            .await?;

        rx.await?
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

    pub async fn basic_get(&self, queue_name: &str, no_ack: bool) -> Result<Message> {
        todo!();
    }

    pub async fn basic_recover(&self, requeue: bool) -> Result<()> {
        Ok(())
    }

    pub async fn select() -> Result<()> {
        Ok(())
    }

    pub async fn add_on_return_callback(&mut self, cb: ReturnCallback) -> Result<()> {
        Ok(())
    }

    pub async fn add_on_cancel_callback(&mut self) -> Result<()> {
        Ok(())
    }

    pub async fn add_on_close_callback(&mut self) -> Result<()> {
        Ok(())
    }

    async fn basic_ack(&self, delivery_tag: u64, multiple: bool) -> Result<()> {
        let frame = frame::basic_ack(self.channel, delivery_tag, multiple);

        self.sink
            .send(client::Request {
                param: client::Param::Frame(frame),
                response: None,
            })
            .await?;

        Ok(())
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
