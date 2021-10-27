use crate::channel_api::{Channel, Message};
use crate::client_api::{ClientRequest, ClientRequestSink, Param, WaitFor};
use crate::client_error;
use crate::model;
use crate::processor;
use anyhow::Result;
use metalmq_codec::frame;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
pub enum ConsumerSignal {
    Delivered(Message),
    Cancelled,
    ChannelClosed,
    ConnectionClosed,
}

pub struct ConsumerHandler {
    pub channel: model::ChannelNumber,
    pub consumer_tag: String,
    client_sink: ClientRequestSink,
    pub signal_stream: mpsc::UnboundedReceiver<ConsumerSignal>,
}

impl ConsumerHandler {
    pub async fn basic_ack(&self, delivery_tag: u64) -> Result<()> {
        // TODO it should wait the ack to be sent out the network - see processor.rs
        processor::send(&self.client_sink, frame::basic_ack(self.channel, delivery_tag, false)).await
    }

    //pub async fn basic_nack(&self, delivery_tag: u64, multiple: bool, requeue: bool) -> Result<()> {
    //    processor::send(&self.client_sink, frame::basic_nack(self.channel, delivery_tag, false)).await
    //}
    //
    //pub async fn reject (delivery tag, requeue)

    pub async fn basic_cancel(self) -> Result<()> {
        let frame = frame::basic_cancel(self.channel, &self.consumer_tag, false);

        processor::call(&self.client_sink, frame).await
    }
}

impl Channel {
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

    pub async fn basic_consume<'a>(
        &'a self,
        queue_name: &'a str,
        consumer_tag: &'a str,
        flags: Option<frame::BasicConsumeFlags>,
    ) -> Result<ConsumerHandler> {
        let frame = frame::basic_consume(self.channel, queue_name, consumer_tag, flags);

        // Buffer of the incoming, delivered messages or other signals like
        // consumer cancelled.
        let (signal_sink, signal_stream) = mpsc::unbounded_channel::<ConsumerSignal>();

        let handler = ConsumerHandler {
            channel: self.channel,
            consumer_tag: consumer_tag.to_owned(),
            client_sink: self.sink.clone(),
            signal_stream,
        };

        let (tx, rx) = oneshot::channel();

        self.sink
            .send(ClientRequest {
                param: Param::Consume(frame, signal_sink),
                response: WaitFor::FrameResponse(tx),
            })
            .await?;

        match rx.await {
            Ok(response) => match response {
                Ok(()) => Ok(handler),
                Err(e) => Err(e),
            },
            Err(_) => client_error!(None, 501, "Channel recv error", 0),
        }
    }
}
