use crate::client_api::{ClientRequest, ClientRequestSink, Param, WaitFor};
use crate::client_error;
use crate::model::ChannelNumber;
use crate::processor;
use anyhow::Result;
use metalmq_codec::frame;
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

#[derive(Debug)]
pub struct Channel {
    channel: ChannelNumber,
    sink: ClientRequestSink,
    /// Active consumers by consumer tag
    consumers: HashMap<String, ClientRequest>,
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

// TODO create functions which generates these value
pub struct ConsumerResponse<T> {
    pub result: Option<T>,
    pub ack: ConsumerAck,
}

pub type ConsumerFn<T> = dyn FnMut(ConsumerSignal) -> ConsumerResponse<T> + Send + Sync;

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
        let (tx, rx) = oneshot::channel();

        self.sink
            .send(ClientRequest {
                param: Param::Publish(frame, payload.as_bytes().to_vec()),
                response: WaitFor::SentOut(tx),
            })
            .await?;

        rx.await?
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

    pub async fn basic_consume<'a, T: std::fmt::Debug + Send + 'static>(
        &'a self,
        queue_name: &'a str,
        consumer_tag: &'a str,
        flags: Option<frame::BasicConsumeFlags>,
        mut consumer: Box<ConsumerFn<T>>,
    ) -> Result<JoinHandle<Option<T>>> {
        let frame = frame::basic_consume(self.channel, queue_name, consumer_tag, flags);
        let (tx, rx) = oneshot::channel();

        // TODO this will be the buffer of the inflight messages
        // FIXME if it is smaller that the messages we want to receive, it hangs :(
        let (sink, mut stream) = mpsc::unbounded_channel::<ConsumerSignal>();

        // Clone the channel in order that users can use this ClientChannel
        // to publish messages.
        let client_request_sink = self.sink.clone();
        let channel_number = self.channel;

        let join_handle: JoinHandle<Option<T>> = tokio::spawn(async move {
            // Result of the oneshot channel what the user listens for the result of the
            // whole consume process.
            let mut final_result: Option<T> = None;

            loop {
                match stream.recv().await {
                    Some(signal) => {
                        match consumer(signal) {
                            ConsumerResponse { result, ack } => {
                                if let Some(_) = result {
                                    final_result = result;
                                }

                                match ack {
                                    ConsumerAck::Ack { delivery_tag, multiple } => {
                                        // FIXME this should not be public api, so no channel
                                        // struct needed
                                        let ack_frame = frame::basic_ack(channel_number, delivery_tag, false);
                                        let (tx, rx) = oneshot::channel();

                                        client_request_sink
                                            .send(ClientRequest {
                                                param: Param::Frame(ack_frame),
                                                response: WaitFor::SentOut(tx),
                                            })
                                            .await;

                                        rx.await;

                                        ()
                                    }
                                    ConsumerAck::Nothing => {
                                        break;
                                    }
                                    _ => unimplemented!(),
                                }
                            }
                        };
                    }
                    None => {
                        let ConsumerResponse { result, .. } = consumer(ConsumerSignal::Cancelled);

                        if let Some(r) = result {
                            final_result = Some(r);
                        }

                        break;
                    }
                }
            }

            final_result
        });

        self.sink
            .send(ClientRequest {
                param: Param::Consume(frame, sink),
                response: WaitFor::FrameResponse(tx),
            })
            .await?;

        match rx.await {
            Ok(response) => match response {
                Ok(()) => Ok(join_handle),
                Err(e) => Err(e),
            },
            Err(_) => client_error!(None, 501, "Channel recv error", 0),
        }
    }

    pub async fn basic_cancel(&self, consumer_tag: &str) -> Result<()> {
        let frame = frame::basic_cancel(self.channel, consumer_tag, false);
        let (tx, rx) = oneshot::channel();

        self.sink
            .send(ClientRequest {
                param: Param::Frame(frame),
                response: WaitFor::FrameResponse(tx),
            })
            .await?;

        rx.await?
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
