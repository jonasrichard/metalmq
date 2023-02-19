use anyhow::Result;
use std::collections::HashMap;

use crate::message::PublishedMessage;
use crate::model::ChannelNumber;
use crate::processor;
use crate::processor::{ClientRequest, ClientRequestSink, Param};
use metalmq_codec::frame;

pub enum ChannelState {
    Open,
    Closing,
    Closed,
}

/// A channel is the main method of communicating with an AMQP server. Channels can be created on
/// an open connection by calling the [`Client.channel_open`] function.
pub struct Channel {
    /// Channel number identifies the channel in a connection.
    pub channel: ChannelNumber,
    /// The status of the channel.
    pub state: ChannelState,
    pub(crate) sink: ClientRequestSink,
}

impl std::fmt::Debug for Channel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Channel").field("channel", &self.channel).finish()
    }
}

/// Represents the exchange binding type during `Queue.Bind`
pub enum ExchangeType {
    /// Messages are sent to the queue if the message routing key is equal to the binding routing
    /// key.
    Direct,
    /// Messages are sent to all bound queue and the routing key is ignored.
    Fanout,
    /// Message routing key are matched to the routing key pattern of different queues bound to the
    /// exchange, and the message is forwarded to the matching queues only. For example if the
    /// message routing key is `stock.nyse.goog` the matching routing keys are `stock.*.*`,
    /// `stock.nyse.*` or `stock.#` where hashmark matches more tags.
    Topic,
    /// Here the headers of the message are matched to the criteria defined by the binding. All or
    /// any match is enough, based on the configuration.
    Headers,
}

impl From<ExchangeType> for &'static str {
    fn from(et: ExchangeType) -> &'static str {
        match et {
            ExchangeType::Direct => "direct",
            ExchangeType::Fanout => "fanout",
            ExchangeType::Topic => "topic",
            ExchangeType::Headers => "headers",
        }
    }
}

/// Condition for stating that queue can be deleted if it is empty, doesn't have messages.
pub struct IfEmpty(pub bool);
/// Condition for deleting an exchange or a queue if they don't have active consumers.
pub struct IfUnused(pub bool);

/// Options for declaring exchanges.
///
/// It works in a builder style, one can chain the setter functions to build the option.
/// ```no_run
/// use metalmq_client::*;
///
/// async fn declare_exchange(channel: Channel) {
///     channel.exchange_declare(
///         "number-plates",
///         ExchangeType::Direct,
///         ExchangeDeclareOpts::default().durable(true)
///         )
///         .await
///         .unwrap();
/// }
/// ```
#[derive(Default)]
pub struct ExchangeDeclareOpts {
    passive: bool,
    durable: bool,
    auto_delete: bool,
    internal: bool,
}

impl ExchangeDeclareOpts {
    /// In passive declare the client can check if the exchange exists and it has been declared
    /// with the same parameters (durable, auto delete and internal).
    pub fn passive(mut self, mode: bool) -> Self {
        self.passive = mode;
        self
    }

    /// Durable exchanges survive the server restart.
    pub fn durable(mut self, mode: bool) -> Self {
        self.durable = mode;
        self
    }

    /// `AutoDelete` queues are deleted when no longer used. When the last consumer closes the
    /// connection, the server deletes the queue. Auto delete queues can be deleted explicitly. Auto
    /// delete queues are not deleted if they are not yet used.
    pub fn auto_delete(mut self, mode: bool) -> Self {
        self.auto_delete = mode;
        self
    }

    /// Clients cannot publish to internal exchanges only they can be bound to other exchanges and
    /// exchanges can forward messages to them.
    pub fn internal(mut self, mode: bool) -> Self {
        self.internal = mode;
        self
    }
}

/// Builder style helper to specify options during queue declaration.
///
/// See [`ExchangeDeclareOpts`]
#[derive(Default)]
pub struct QueueDeclareOpts {
    passive: bool,
    durable: bool,
    exclusive: bool,
    auto_delete: bool,
}

impl QueueDeclareOpts {
    /// In passive declare the client can check if the queue exists and it has been declared
    /// with the same parameters (durable, auto delete and exclusive).
    pub fn passive(mut self, mode: bool) -> Self {
        self.passive = mode;
        self
    }

    /// Durable queues survive the server restart.
    pub fn durable(mut self, mode: bool) -> Self {
        self.durable = mode;
        self
    }

    /// Exclusive queues can be access by the declaring connection only, and they are deleted after
    /// the connection terminates.
    pub fn exclusive(mut self, mode: bool) -> Self {
        self.exclusive = mode;
        self
    }

    /// Auto delete queues are deleted after the last consumer terminates. If a queue has not been
    /// consumed, it won't be deleted even if the declaring connection terminates.
    pub fn auto_delete(mut self, mode: bool) -> Self {
        self.auto_delete = mode;
        self
    }
}

/// Describe how many headers need to be matched in case of headers binding.
#[derive(Debug)]
pub enum HeaderMatch {
    /// Any non `x-` header match is suffice.
    Any,
    /// All non `x-` header need to match.
    All,
    /// Any header match result in a success.
    AnyWithX,
    /// All header need to match including the ones start with `x-`.
    AllWithX,
}

impl From<HeaderMatch> for frame::AMQPFieldValue {
    fn from(value: HeaderMatch) -> Self {
        match value {
            HeaderMatch::Any => frame::AMQPFieldValue::LongString(String::from("any")),
            HeaderMatch::All => frame::AMQPFieldValue::LongString(String::from("all")),
            HeaderMatch::AnyWithX => frame::AMQPFieldValue::LongString(String::from("any-with-x")),
            HeaderMatch::AllWithX => frame::AMQPFieldValue::LongString(String::from("all-with-x")),
        }
    }
}

/// Describe the queue binding.
#[derive(Debug)]
pub enum Binding {
    /// Direct binding routes messages to a bound queue if the routing key of the message equals to
    /// the routing key in the binding.
    Direct(String),
    /// In topic binding the message routed if the routing key of the message conforms to the topic
    /// exchange pattern. The pattern can be an exact routing key in this case the match is
    /// equality. It can contain asterisks which means any string separated by dots, like "stock.*.*"
    /// matches "stock.nyse.goog". It can contain hashmark which covers multiple dot-separated
    /// parts, like "stock.#" matchs to all substrings including one with dots.
    Topic(String),
    /// Fanout exchange broadcast message to each bound queues.
    Fanout,
    /// Headers binding matches the messages by their headers. Any or all header should match
    /// depending on the [`HeaderMatch`] value. Currently header values are `String`s only.
    Headers {
        headers: HashMap<String, String>,
        x_match: HeaderMatch,
    },
}

impl Channel {
    pub(crate) fn new(channel: ChannelNumber, sink: ClientRequestSink) -> Channel {
        Channel {
            channel,
            state: ChannelState::Open,
            sink,
        }
    }

    /// Declare exchange.
    pub async fn exchange_declare(
        &self,
        exchange_name: &str,
        exchange_type: ExchangeType,
        opts: ExchangeDeclareOpts,
    ) -> Result<()> {
        let frame = frame::ExchangeDeclareArgs::default()
            .exchange_name(exchange_name)
            .exchange_type(exchange_type.into())
            .passive(opts.passive)
            .durable(opts.durable)
            .auto_delete(opts.auto_delete)
            .internal(opts.internal)
            .frame(self.channel);

        processor::call(&self.sink, frame).await
    }

    /// Delete exchange.
    ///
    /// ```no_run
    /// use metalmq_client::{Client, IfUnused};
    ///
    /// # async fn foo() {
    /// let (mut c, _) = Client::connect("localhost:5672", "guest", "guest").await.unwrap();
    /// let ch = c.channel_open(1).await.unwrap();
    ///
    /// ch.exchange_delete("price-exchange", IfUnused(false)).await.unwrap();
    /// # }
    /// ```
    pub async fn exchange_delete(&self, exchange_name: &str, if_unused: IfUnused) -> Result<()> {
        let frame = frame::ExchangeDeleteArgs::default()
            .exchange_name(exchange_name)
            .if_unused(if_unused.0)
            .frame(self.channel);

        processor::call(&self.sink, frame).await
    }

    /// Declare queue.
    pub async fn queue_declare(&self, queue_name: &str, opts: QueueDeclareOpts) -> Result<()> {
        let frame = frame::QueueDeclareArgs::default()
            .name(queue_name)
            .passive(opts.passive)
            .durable(opts.durable)
            .exclusive(opts.exclusive)
            .auto_delete(opts.auto_delete)
            .frame(self.channel);

        processor::call(&self.sink, frame).await
    }

    /// Bind queue to exchange.
    pub async fn queue_bind(&self, queue_name: &str, exchange_name: &str, binding: Binding) -> Result<()> {
        use frame::AMQPFieldValue;

        let mut queue_binding = frame::QueueBindArgs::new(queue_name, exchange_name);

        queue_binding = match binding {
            Binding::Direct(routing_key) => queue_binding.routing_key(&routing_key),
            Binding::Topic(routing_key) => queue_binding.routing_key(&routing_key),
            Binding::Fanout => queue_binding,
            Binding::Headers { headers, x_match } => {
                let mut args = HashMap::new();

                for (k, v) in headers.into_iter() {
                    args.insert(k, AMQPFieldValue::LongString(v));
                }

                args.insert("x-match".to_string(), x_match.into());

                queue_binding.args = Some(args);
                queue_binding
            }
        };

        processor::call(&self.sink, queue_binding.frame(self.channel)).await
    }

    pub async fn queue_unbind(&self, queue_name: &str, exchange_name: &str, routing_key: &str) -> Result<()> {
        let frame = frame::QueueUnbindArgs::new(queue_name, exchange_name)
            .routing_key(routing_key)
            .frame(self.channel);

        processor::call(&self.sink, frame).await
    }

    pub async fn queue_purge(&self, queue_name: &str) -> Result<()> {
        // FIXME give back the number of messages arriving in the QueuePurgeOk frame
        processor::call(
            &self.sink,
            frame::QueuePurgeArgs::default()
                .queue_name(queue_name)
                .frame(self.channel),
        )
        .await
    }

    pub async fn queue_delete(&self, queue_name: &str, if_unused: IfUnused, if_empty: IfEmpty) -> Result<()> {
        let frame = frame::QueueDeleteArgs::default()
            .queue_name(queue_name)
            .if_empty(if_empty.0)
            .if_unused(if_unused.0)
            .frame(self.channel);

        processor::call(&self.sink, frame).await
    }

    pub async fn basic_publish<T>(&self, exchange_name: &str, routing_key: &str, message: T) -> Result<()>
    where
        T: Into<PublishedMessage>,
    {
        let message = message.into();
        let frame = frame::BasicPublishArgs::new(exchange_name)
            .routing_key(routing_key)
            .immediate(message.immediate)
            .mandatory(message.mandatory)
            .frame(self.channel);

        self.sink
            .send(ClientRequest {
                param: Param::Publish(Box::new(frame), Box::new(message.message)),
                response: None,
            })
            .await?;

        Ok(())
    }

    pub async fn confirm(&self) -> Result<()> {
        processor::call(&self.sink, frame::confirm_select(self.channel)).await
    }

    /// Closes the channel.
    pub async fn close(&mut self) -> Result<()> {
        self.state = ChannelState::Closing;

        // TODO notify the Client struct and let it remove the channel from its hashmap
        processor::call(
            &self.sink,
            frame::channel_close(self.channel, 200, "Normal close", frame::CHANNEL_CLOSE),
        )
        .await?;

        self.state = ChannelState::Closed;

        Ok(())
    }
}
