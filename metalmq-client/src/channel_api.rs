use crate::client_api::{ClientRequest, ClientRequestSink, Param};
use crate::model::ChannelNumber;
use crate::processor;
use anyhow::Result;
use metalmq_codec::frame;
use std::collections::HashMap;
use tokio::sync::mpsc;

#[derive(Debug)]
pub struct Channel {
    channel: ChannelNumber,
    sink: ClientRequestSink,
    /// Active consumers by consumer tag
    consumers: HashMap<String, mpsc::Sender<ConsumerSignal>>,
}

#[derive(Debug)]
pub struct Message {
    pub channel: ChannelNumber,
    pub consumer_tag: String,
    pub delivery_tag: u64,
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

#[derive(Debug)]
pub enum ConsumerSignal {
    Delivered(Message),
    Cancelled,
    ChannelClosed,
    ConnectionClosed,
}

pub enum ConsumerAck {
    Ack,
    Nack,
    Reject,
    Nothin,
}

pub struct ConsumerResponse<T> {
    result: Option<T>,
    ack: ConsumerAck,
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
        exchange_type: &str,
        flags: Option<frame::ExchangeDeclareFlags>,
    ) -> Result<()> {
        let frame = frame::exchange_declare(self.channel, exchange_name, exchange_type, flags);

        processor::call(&self.sink, frame).await
    }

    /// Delete exchange.
    pub async fn exchange_delete(&self, exchange_name: &str, if_unused: bool) -> Result<()> {
        let mut flags = frame::ExchangeDeleteFlags::default();

        if if_unused {
            flags.toggle(frame::ExchangeDeleteFlags::IF_UNUSED);
        }

        let frame = frame::exchange_delete(self.channel, exchange_name, Some(flags));

        processor::call(&self.sink, frame).await
    }

    /// Declare queue.
    pub async fn queue_declare(&self, queue_name: &str, flags: Option<frame::QueueDeclareFlags>) -> Result<()> {
        let frame = frame::queue_declare(self.channel, queue_name, flags);

        processor::call(&self.sink, frame).await
    }

    /// Bind queue to exchange.
    pub async fn queue_bind(&self, queue_name: &str, exchange_name: &str, routing_key: &str) -> Result<()> {
        let frame = frame::queue_bind(self.channel, queue_name, exchange_name, routing_key);

        processor::call(&self.sink, frame).await
    }

    pub async fn queue_unbind(&self, queue_name: &str, exchange_name: &str, routing_key: &str) -> Result<()> {
        let frame = frame::queue_unbind(self.channel, queue_name, exchange_name, routing_key);

        processor::call(&self.sink, frame).await
    }

    pub async fn queue_purge(&self, queue_name: &str) -> Result<()> {
        Ok(())
    }

    pub async fn queue_delete(&self, queue_name: &str, if_unused: bool, if_empty: bool) -> Result<()> {
        let mut flags = frame::QueueDeleteFlags::empty();
        flags.set(frame::QueueDeleteFlags::IF_UNUSED, if_unused);
        flags.set(frame::QueueDeleteFlags::IF_EMPTY, if_empty);

        let frame = frame::queue_delete(self.channel, queue_name, Some(flags));

        processor::call(&self.sink, frame).await
    }

    pub async fn basic_publish(&self, exchange_name: &str, routing_key: &str, payload: String) -> Result<()> {
        let frame = frame::basic_publish(self.channel, exchange_name, routing_key);

        self.sink
            .send(ClientRequest {
                param: Param::Publish(frame, payload.as_bytes().to_vec()),
                response: None,
            })
            .await?;

        Ok(())
    }

    /// Closes the channel.
    pub async fn close(&self) -> Result<()> {
        let (cid, mid) = frame::split_class_method(frame::CHANNEL_CLOSE);

        processor::call(
            &self.sink,
            frame::channel_close(self.channel, 200, "Normal close", cid, mid),
        )
        .await
    }
}
