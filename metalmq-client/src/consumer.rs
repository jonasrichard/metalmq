use std::time::Duration;

use crate::channel_api::{Channel, Message};
use crate::client_error;
use crate::model;
use crate::processor::{self, ClientRequest, ClientRequestSink, Param, WaitFor};
use anyhow::Result;
use metalmq_codec::frame;
use tokio::sync::{mpsc, oneshot};

/// A signal arriving from the server during consuming a queue.
#[derive(Debug)]
pub enum ConsumerSignal {
    Delivered(Message),
    Cancelled,
    ChannelClosed,
    ConnectionClosed,
}

/// Consumer API for `Basic.Consume`.
///
/// `ConsumerHandler` can be get by invoking [`Channel::basic_consume`].
pub struct ConsumerHandler {
    /// The channel number we are consuming messages. One client can have one consumer per channel.
    pub channel: model::ChannelNumber,
    /// Identifier of the consumer in server.
    pub consumer_tag: String,
    client_sink: ClientRequestSink,
    /// From this signal stream the consumer gets the messages as [`ConsumerSignal`] values and can
    /// handle them by acking messages or handling channel or connection close events.
    pub signal_stream: mpsc::UnboundedReceiver<ConsumerSignal>,
}

/// After consuming started with `ConsumerHandler` one can ack, nack or reject messages.
///
/// ```no_run
/// use metalmq_client::{Channel, ConsumerSignal, Exclusive, NoAck, NoLocal};
///
/// async fn consume(channel: Channel) {
///     let mut handler = channel.basic_consume("queue", "consumer-tag-1", NoAck(false),
///         Exclusive(false), NoLocal(false)).await.unwrap();
///
///     while let Some(signal) = handler.signal_stream.recv().await {
///         match signal {
///             ConsumerSignal::Delivered(m) => {
///                 handler.basic_ack(m.delivery_tag).await.unwrap();
///             }
///             ConsumerSignal::Cancelled | ConsumerSignal::ChannelClosed |
///                 ConsumerSignal::ConnectionClosed => {
///                 break;
///             }
///         }
///     }
/// }
/// ```
impl ConsumerHandler {
    pub async fn receive(&mut self, timeout: Duration) -> Option<ConsumerSignal> {
        let sleep = tokio::time::sleep(tokio::time::Duration::from(timeout));
        tokio::pin!(sleep);

        tokio::select! {
            signal = self.signal_stream.recv() => {
                signal
            }
            _ = &mut sleep => {
                return None;
            }
        }
    }

    pub async fn basic_ack(&self, delivery_tag: u64) -> Result<()> {
        processor::sync_send(
            &self.client_sink,
            frame::BasicAckArgs::default()
                .delivery_tag(delivery_tag)
                .multiple(false)
                .frame(self.channel),
        )
        .await
    }

    //pub async fn basic_nack(&self, delivery_tag: u64, multiple: bool, requeue: bool) -> Result<()> {
    //    processor::send(&self.client_sink, frame::basic_nack(self.channel, delivery_tag, false)).await
    //}
    //
    //pub async fn reject (delivery tag, requeue)

    pub async fn basic_cancel(self) -> Result<()> {
        let frame = frame::BasicCancelArgs::new(&self.consumer_tag).frame(self.channel);

        processor::call(&self.client_sink, frame).await
    }
}

pub struct Exclusive(pub bool);
pub struct NoAck(pub bool);
pub struct NoLocal(pub bool);

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

    // TODO ConsumerTag should be an enum with a value or we can ask client to generate a ctag
    /// See [`ConsumerHandler`]
    pub async fn basic_consume<'a>(
        &'a self,
        queue_name: &'a str,
        consumer_tag: &'a str,
        no_ack: NoAck,
        exclusive: Exclusive,
        no_local: NoLocal,
    ) -> Result<ConsumerHandler> {
        let frame = frame::BasicConsumeArgs::default()
            .queue(queue_name)
            .consumer_tag(consumer_tag)
            .no_ack(no_ack.0)
            .exclusive(exclusive.0)
            .no_local(no_local.0)
            .frame(self.channel);

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
