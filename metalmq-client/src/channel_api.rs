use crate::client_api::{ClientRequest, ClientRequestSink, Param, WaitFor};
use crate::model::ChannelNumber;
use crate::processor;
use anyhow::Result;
use metalmq_codec::frame;
use std::collections::HashMap;

/// A channel is the main method of communicating with an AMQP server. Channels can be created on
/// an open connection by calling the [`Client::channel_open`] function.
pub struct Channel {
    /// Channel number identifies the channel in a connection.
    pub(crate) channel: ChannelNumber,
    pub(crate) sink: ClientRequestSink,
    /// Active consumers by consumer tag
    consumers: HashMap<String, ClientRequest>,
}

impl std::fmt::Debug for Channel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Channel")
            .field("channel", &(self.channel as u16))
            .finish()
    }
}

/// A message received from the server.
///
/// With the `consumer_tag` and `delivery_tag` a client can send back acknowledgements to the
/// server, saying that the message was successfully arrived.
#[derive(Debug)]
pub struct Message {
    pub channel: ChannelNumber,
    pub consumer_tag: String,
    pub delivery_tag: u64,
    // TODO put routing key and properties here and all the things from the message header
    pub length: usize,
    pub body: Vec<u8>,
}

/// The temporary data structure for collecting message details from different AMQP frames like
/// Basic.Deliver or Basic.Return and ContentHeader and ContentBody frames. Those frames are sent
/// consequtively in a channel, so the client should collect them. This is low-level functionality,
/// this shouldn't be visible to the client.
#[derive(Debug)]
pub(crate) struct DeliveredContent {
    channel: ChannelNumber,
    consumer_tag: String,
    delivery_tag: u64,
    exchange_name: String,
    routing_key: String,
    body_size: Option<u64>,
    body: Option<Vec<u8>>,
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
/// Condition for immediate publishing. Immediate messages are received by a server successfully if
/// they managed to be sent to a consumer immediately.
pub struct Immediate(pub bool);
/// Condition for mandatory publishing. Mandatory messages are failed if the exchange doesn't have
/// bound queue or if the routing keys are not matched.
pub struct Mandatory(pub bool);

#[derive(Default)]
pub struct ExchangeDeclareOpts {
    passive: bool,
    durable: bool,
    /// `AutoDelete` queues are deleted when no longer used. When the last consumer closes the
    /// connection, the server deletes the queue. Auto delete queues can be deleted explicitly. Auto
    /// delete queues are not deleted if they are not yet used.
    auto_delete: bool,
    internal: bool,
}

impl ExchangeDeclareOpts {
    pub fn passive(mut self, mode: bool) -> Self {
        self.passive = mode;
        self
    }

    pub fn durable(mut self, mode: bool) -> Self {
        self.durable = mode;
        self
    }

    pub fn auto_delete(mut self, mode: bool) -> Self {
        self.auto_delete = mode;
        self
    }

    pub fn internal(mut self, mode: bool) -> Self {
        self.internal = mode;
        self
    }
}

#[derive(Default)]
pub struct QueueDeclareOpts {
    passive: bool,
    durable: bool,
    exclusive: bool,
    auto_delete: bool,
}

impl QueueDeclareOpts {
    pub fn passive(mut self, mode: bool) -> Self {
        self.passive = mode;
        self
    }

    pub fn durable(mut self, mode: bool) -> Self {
        self.durable = mode;
        self
    }

    pub fn exclusive(mut self, mode: bool) -> Self {
        self.exclusive = mode;
        self
    }

    pub fn auto_delete(mut self, mode: bool) -> Self {
        self.auto_delete = mode;
        self
    }
}

impl Channel {
    pub(crate) fn new(channel: ChannelNumber, sink: ClientRequestSink) -> Channel {
        Channel {
            channel,
            sink,
            consumers: HashMap::new(),
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
    /// use metalmq_client::IfUnused;
    ///
    /// # async fn foo() {
    /// let mut c = metalmq_client::connect("localhost:5672", "guest", "guest").await.unwrap();
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
    pub async fn queue_bind(&self, queue_name: &str, exchange_name: &str, routing_key: &str) -> Result<()> {
        let frame = frame::QueueBindArgs::new(queue_name, exchange_name)
            .routing_key(routing_key)
            .frame(self.channel);

        processor::call(&self.sink, frame).await
    }

    pub async fn queue_unbind(&self, queue_name: &str, exchange_name: &str, routing_key: &str) -> Result<()> {
        let frame = frame::QueueUnbindArgs::new(queue_name, exchange_name)
            .routing_key(routing_key)
            .frame(self.channel);

        processor::call(&self.sink, frame).await
    }

    pub async fn queue_purge(&self, queue_name: &str) -> Result<()> {
        Ok(())
    }

    pub async fn queue_delete(&self, queue_name: &str, if_unused: IfUnused, if_empty: IfEmpty) -> Result<()> {
        let frame = frame::QueueDeleteArgs::default()
            .queue_name(queue_name)
            .if_empty(if_empty.0)
            .if_unused(if_unused.0)
            .frame(self.channel);

        processor::call(&self.sink, frame).await
    }

    pub async fn basic_publish(
        &self,
        exchange_name: &str,
        routing_key: &str,
        payload: String,
        mandatory: Mandatory,
        immediate: Immediate,
    ) -> Result<()> {
        let frame = frame::BasicPublishArgs::new(exchange_name)
            .routing_key(routing_key)
            .immediate(immediate.0)
            .mandatory(mandatory.0)
            .frame(self.channel);

        self.sink
            .send(ClientRequest {
                param: Param::Publish(frame, payload.as_bytes().to_vec()),
                response: WaitFor::Nothing,
            })
            .await?;

        Ok(())
    }

    /// Closes the channel.
    pub async fn close(&self) -> Result<()> {
        processor::call(
            &self.sink,
            frame::channel_close(self.channel, 200, "Normal close", frame::CHANNEL_CLOSE),
        )
        .await
    }
}
