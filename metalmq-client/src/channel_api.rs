use crate::client_api::{ClientRequest, ClientRequestSink, Param, WaitFor};
use crate::model::ChannelNumber;
use crate::processor;
use anyhow::Result;
use metalmq_codec::frame;
use std::collections::HashMap;

pub struct Channel {
    pub(crate) channel: ChannelNumber,
    pub(crate) sink: ClientRequestSink,
    /// Active consumers by consumer tag
    pub(crate) consumers: HashMap<String, ClientRequest>,
}

impl std::fmt::Debug for Channel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Channel")
            .field("channel", &(self.channel as u16))
            .finish()
    }
}

#[derive(Debug)]
pub struct Message {
    pub channel: ChannelNumber,
    pub consumer_tag: String,
    pub delivery_tag: u64,
    // TODO put routing key and properties here and all the things from the message header
    pub length: usize,
    pub body: Vec<u8>,
}

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

pub enum ExchangeType {
    Direct,
    Fanout,
    Topic,
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

pub struct Exclusive(pub bool);
pub struct IfEmpty(pub bool);
pub struct IfUnused(pub bool);
pub struct Immediate(pub bool);
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
