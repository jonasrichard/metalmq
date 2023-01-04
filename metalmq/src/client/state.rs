// Do we need to expose the messages of a 'process' or hide it in an erlang-style?
use crate::client;
use crate::exchange::handler::ExchangeCommandSink;
use crate::exchange::manager as em;
use crate::queue::handler as queue_handler;
use crate::queue::manager as qm;
use crate::{logerr, Context, ErrorScope, Result, RuntimeError};
use log::{error, info, trace};
use metalmq_codec::codec::Frame;
use metalmq_codec::frame::ContentBodyFrame;
use metalmq_codec::frame::{Channel, ContentHeaderFrame};
use std::collections::HashMap;
use tokio::sync::mpsc;
use uuid::Uuid;

pub mod basic;
pub mod channel;
pub mod connect;
pub mod exchange;
pub mod queue;

#[cfg(test)]
mod tests;

/// Queues consumed by the connection with Basic.Consume
#[derive(Debug)]
struct ActivelyConsumedQueue {
    queue_name: String,
    consumer_tag: String,
    queue_sink: queue_handler::QueueCommandSink,
}

/// Queues consumed by the connection with Basic.Get
#[derive(Debug)]
struct PassivelyConsumedQueue {
    delivery_tag: u64,
    queue_sink: queue_handler::QueueCommandSink,
}

/// All the transient data of a connection are stored here.
pub struct Connection {
    /// Unique ID of the connection.
    id: String,
    qm: qm::QueueManagerSink,
    em: em::ExchangeManagerSink,
    /// The highest channel number, 0 if there is no limit.
    channel_max: u16,
    /// The maximal size of frames the client can accept.
    frame_max: usize,
    /// How frequently the server sends heartbeat (at most).
    heartbeat_interval: Option<std::time::Duration>,
    /// Opened channels by this connection.
    open_channels: HashMap<Channel, ChannelState>,
    /// Declared exchanges by this connection.
    exchanges: HashMap<String, ExchangeCommandSink>,
    /// Exchanges which declared by this channel as auto-delete
    auto_delete_exchanges: Vec<String>,
    /// Consumed queues by this connection. One channel can have one queue consumed.
    consumed_queues: HashMap<Channel, ActivelyConsumedQueue>,
    /// Passively consumed queues by Basic.Get
    passively_consumed_queues: HashMap<Channel, PassivelyConsumedQueue>,
    /// Incoming messages come in different messages, we need to collect their properties
    in_flight_contents: HashMap<Channel, PublishedContent>,
    /// Sink for AMQP frames toward the client
    outgoing: mpsc::Sender<Frame>,
}

/// Represents a channel
#[derive(Debug)]
pub struct ChannelState {
    /// The channel number
    pub channel: Channel,
    /// Whether the channel is in confirm mode. The default is false, so no. In confirm mode the
    /// server can send Ack messages to the client (basic return for example).
    pub confirm_mode: bool,
    /// The outgoing frame channel.
    pub frame_sink: mpsc::Sender<Frame>,
}

#[derive(Debug, Default)]
struct PublishedContent {
    channel: Channel,
    exchange: String,
    routing_key: String,
    mandatory: bool,
    immediate: bool,
    /// The method frame class id which initiated the sending of the content.
    method_frame_class_id: u32,
    content_header: ContentHeaderFrame,
    content_bodies: Vec<ContentBodyFrame>,
    body_size: usize,
}

#[macro_export]
macro_rules! handle_error {
    ($self:expr, $val:expr) => {
        match $val {
            ok @ Ok(_) => ok,
            Err(e) => {
                return $self.handle_error(*e.downcast::<$crate::RuntimeError>().unwrap()).await;
            }
        }
    };
}

pub fn new(context: Context, outgoing: mpsc::Sender<Frame>) -> Connection {
    let conn_id = Uuid::new_v4().as_hyphenated().to_string();

    info!("Client connected id = {conn_id}");

    Connection {
        id: conn_id,
        qm: context.queue_manager,
        em: context.exchange_manager,
        channel_max: 2047,
        frame_max: 131_072,
        heartbeat_interval: None,
        open_channels: HashMap::new(),
        exchanges: HashMap::new(),
        auto_delete_exchanges: vec![],
        consumed_queues: HashMap::new(),
        passively_consumed_queues: HashMap::new(),
        in_flight_contents: HashMap::new(),
        outgoing,
    }
}

impl Connection {
    /// Send frame out to client asynchronously.
    pub async fn send_frame(&self, f: Frame) -> Result<()> {
        self.outgoing.send(f).await?;

        Ok(())
    }

    async fn handle_connection_close(&mut self) -> Result<()> {
        for (channel, cq) in self.consumed_queues.drain() {
            let cmd = qm::QueueCancelConsume {
                channel,
                queue_name: cq.queue_name.clone(),
                consumer_tag: cq.consumer_tag.clone(),
            };

            logerr!(qm::cancel_consume(&self.qm, cmd).await);
        }

        // TODO cleanup, like close all channels, delete temporal queues, etc
        Ok(())
    }

    async fn handle_channel_close(&mut self, channel: Channel) -> Result<()> {
        // Cancel consumed queues on the channel
        if let Some(cq) = self.consumed_queues.remove(&channel) {
            qm::cancel_consume(
                &self.qm,
                qm::QueueCancelConsume {
                    channel,
                    queue_name: cq.queue_name.clone(),
                    consumer_tag: cq.consumer_tag.clone(),
                },
            )
            .await?;
        }

        Ok(())
    }

    /// Handle a runtime error a connection or a channel error. At first it sends the error frame
    /// and then handle the closing of a channel or connection depending what kind of exception
    /// happened.
    ///
    /// This function just sends out the error frame and return with `Err` if it is a connection
    /// error, or it returns with `Ok` if it is a channel error. This is handy if we want to handle
    /// the output with a `?` operator and we want to die in case of a connection error (aka we
    /// want to propagate the error to the client handler).
    async fn handle_error(&mut self, err: RuntimeError) -> Result<()> {
        trace!("Handling error {:?}", err);

        self.send_frame(client::runtime_error_to_frame(&err)).await?;

        match err.scope {
            ErrorScope::Connection => {
                self.handle_connection_close().await?;

                Err(Box::new(err))
            }
            ErrorScope::Channel => {
                self.handle_channel_close(err.channel).await?;

                Ok(())
            }
        }
    }

    pub(crate) fn get_heartbeat(&self) -> Option<std::time::Duration> {
        self.heartbeat_interval
    }
}
