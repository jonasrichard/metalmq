use std::time::Duration;

use crate::{
    channel_api::Channel,
    client_error,
    message::{DeliveredMessage, GetMessage},
    model,
    processor::{self, ClientRequest, ClientRequestSink, Param, WaitFor},
};
use anyhow::Result;
use metalmq_codec::frame;
use tokio::sync::{mpsc, oneshot};

/// A signal arriving from the server during consuming a queue.
#[derive(Debug)]
pub enum ConsumerSignal {
    Delivered(Box<DeliveredMessage>),
    Cancelled,
    ChannelClosed {
        reply_code: u16,
        reply_text: String,
        class_method: u32,
    },
    ConnectionClosed {
        reply_code: u16,
        reply_text: String,
        class_method: u32,
    },
}

/// A signal for handling result of a `Basic.Get` from the server.
#[derive(Debug)]
pub enum GetSignal {
    GetOk(Box<GetMessage>),
    GetEmpty,
    ChannelClosed {
        reply_code: u16,
        reply_text: String,
        class_method: u32,
    },
    ConnectionClosed {
        reply_code: u16,
        reply_text: String,
        class_method: u32,
    },
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

/// Handler for getting the result of a `Basic.Get`, a passive consume.
pub struct GetHandler {
    /// The channel on which the client doing the get operation.
    pub channel: model::ChannelNumber,
    /// The client should listen the signal stream for the results of the `Basic.Get`. It may get
    /// `GetOk` with the message itself or `GetEmpty` if there is no message in the queue.
    pub signal_stream: mpsc::UnboundedReceiver<GetSignal>,
    client_sink: ClientRequestSink,
}

/// After consuming started with `ConsumerHandler` one can ack, nack or reject messages.
impl ConsumerHandler {
    pub async fn receive(&mut self, timeout: Duration) -> Option<ConsumerSignal> {
        let sleep = tokio::time::sleep(timeout);
        tokio::pin!(sleep);

        tokio::select! {
            signal = self.signal_stream.recv() => {
                signal
            }
            _ = &mut sleep => {
                None
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

    pub async fn basic_nack(&self, delivery_tag: u64, multiple: bool, requeue: bool) -> Result<()> {
        processor::sync_send(
            &self.client_sink,
            frame::BasicNackArgs::default()
                .delivery_tag(delivery_tag)
                .multiple(multiple)
                .requeue(requeue)
                .frame(self.channel),
        )
        .await
    }

    pub async fn basic_reject(&self, delivery_tag: u64, requeue: bool) -> Result<()> {
        processor::sync_send(
            &self.client_sink,
            frame::BasicRejectArgs::default()
                .delivery_tag(delivery_tag)
                .requeue(requeue)
                .frame(self.channel),
        )
        .await
    }

    pub async fn basic_cancel(self) -> Result<()> {
        let frame = frame::BasicCancelArgs::new(&self.consumer_tag).frame(self.channel);

        processor::call(&self.client_sink, frame).await
    }
}

impl GetHandler {
    pub async fn receive(&mut self, timeout: Duration) -> Option<GetSignal> {
        let sleep = tokio::time::sleep(timeout);
        tokio::pin!(sleep);

        tokio::select! {
            signal = self.signal_stream.recv() => {
                signal
            }
            _ = &mut sleep => {
                None
            }
        }
    }

    pub async fn close(self) -> Result<()> {
        // FIXME we need to tell the state that passive consume is over
        // also to the Client?
        Ok(())
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

    async fn basic_nack() {}
    async fn basic_reject() {}
}

/// Specify if the consume is exclusive aka no other client can consume the queue.
pub struct Exclusive(pub bool);
/// Specify if the client needs to ack messages after delivery.
pub struct NoAck(pub bool);
/// Specify if the server sends messages to the same connection which published them.
pub struct NoLocal(pub bool);

impl Channel {
    /// Start consuming a queue.
    ///
    /// It returns a `ConsumerHandler` with which the server events can be handled. Messages are
    /// delivered in the form of those events and also channel close or connection close events
    /// coming through that interface.
    ///
    /// ```no_run
    /// use metalmq_client::{Channel, ConsumerSignal, Exclusive, NoAck, NoLocal};
    ///
    /// async fn consume(channel: Channel) {
    ///     let mut handler = channel.basic_consume("queue", NoAck(false), Exclusive(false),
    ///         NoLocal(false)).await.unwrap();
    ///
    ///     while let Some(signal) = handler.signal_stream.recv().await {
    ///         match signal {
    ///             ConsumerSignal::Delivered(m) => {
    ///                 handler.basic_ack(m.delivery_tag).await.unwrap();
    ///             }
    ///             ConsumerSignal::Cancelled | ConsumerSignal::ChannelClosed { .. } |
    ///                 ConsumerSignal::ConnectionClosed { .. } => {
    ///                 break;
    ///             }
    ///         }
    ///     }
    /// }
    /// ```
    pub async fn basic_consume<'a>(
        &'a self,
        queue_name: &'a str,
        no_ack: NoAck,
        exclusive: Exclusive,
        no_local: NoLocal,
    ) -> Result<ConsumerHandler> {
        let consumer_tag = format!("metalmq-{}", rand::random::<u128>());

        let frame = frame::BasicConsumeArgs::default()
            .queue(queue_name)
            .consumer_tag(&consumer_tag)
            .no_ack(no_ack.0)
            .exclusive(exclusive.0)
            .no_local(no_local.0)
            .frame(self.channel);

        // Buffer of the incoming, delivered messages or other signals like
        // consumer cancelled.
        let (signal_sink, signal_stream) = mpsc::unbounded_channel::<ConsumerSignal>();

        let handler = ConsumerHandler {
            channel: self.channel,
            consumer_tag,
            client_sink: self.sink.clone(),
            signal_stream,
        };

        let (tx, rx) = oneshot::channel();

        self.sink
            .send(ClientRequest {
                param: Param::Consume(Box::new(frame), signal_sink),
                response: Some(WaitFor::FrameResponse(tx)),
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

    pub async fn basic_get(&self, queue_name: &str, no_ack: NoAck) -> Result<GetHandler> {
        let (signal_sink, signal_stream) = mpsc::unbounded_channel();

        let handler = GetHandler {
            channel: self.channel,
            signal_stream,
            client_sink: self.sink.clone(),
        };

        let frame = frame::BasicGetArgs::new(queue_name)
            .no_ack(no_ack.0)
            .frame(self.channel);

        let (tx, rx) = oneshot::channel();

        self.sink
            .send(ClientRequest {
                param: Param::Get(Box::new(frame), signal_sink),
                response: Some(WaitFor::FrameResponse(tx)),
            })
            .await?;

        match rx.await {
            Ok(Ok(())) => Ok(handler),
            Ok(_) | Err(_) => client_error!(None, 501, "Channel recv error", 0),
        }
    }
}
