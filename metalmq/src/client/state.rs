// Do we need to expose the messages of a 'process' or hide it in an erlang-style?
use crate::{
    client,
    client::{channel, ChannelError},
    exchange::{handler::ExchangeCommandSink, manager as em},
    logerr,
    queue::{handler as queue_handler, manager as qm},
    Context, ErrorScope, Result, RuntimeError,
};
use log::{error, info, trace};
use metalmq_codec::{
    codec::Frame,
    frame::{self, Channel, ContentBodyFrame, ContentHeaderFrame},
};
use std::collections::HashMap;
use tokio::sync::mpsc;
use uuid::Uuid;

pub mod basic;
pub mod channel;
pub mod connect;
pub mod exchange;
pub mod queue;
pub mod receive_message;

#[cfg(test)]
mod tests;

// TODO all field of all struct are pub which is not good and it is because of tests

/// Exclusive queue declared by the connection
#[derive(Debug)]
pub struct ExclusiveQueue {
    pub queue_name: String,
}

// TODO we need to create channel struct and channel errors can stop at channel boundary but
// connection errors can be propagated. Also channel errors can have the default channel number.
/// All the transient data of a connection are stored here.
pub struct Connection {
    /// Unique ID of the connection.
    pub id: String,
    pub qm: qm::QueueManagerSink,
    pub em: em::ExchangeManagerSink,
    /// The highest channel number, 0 if there is no limit.
    pub channel_max: u16,
    /// The maximal size of frames the client can accept.
    pub frame_max: usize,
    /// How frequently the server sends heartbeat (at most).
    pub heartbeat_interval: Option<std::time::Duration>,
    /// Opened channels by this connection.
    pub open_channels: HashMap<Channel, ChannelState>,
    /// Declared exchanges by this connection.
    pub exchanges: HashMap<String, ExchangeCommandSink>,
    /// Exchanges which declared by this channel as auto-delete
    pub auto_delete_exchanges: Vec<String>,
    /// Consumed queues by this connection. One channel can have one queue consumed.
    pub consumed_queues: HashMap<Channel, ActivelyConsumedQueue>,
    /// Exclusive queues created by the connection.
    pub exclusive_queues: Vec<ExclusiveQueue>,
    /// Passively consumed queues by Basic.Get
    pub passively_consumed_queues: HashMap<Channel, PassivelyConsumedQueue>,
    /// Incoming messages come in different messages, we need to collect their properties
    pub in_flight_contents: HashMap<Channel, PublishedContent>,
    /// If a channel is in confirm mode, what is the next delivery tag with which it can ack the
    /// message.
    pub next_confirm_delivery_tag: HashMap<Channel, u64>,
    /// Sink for AMQP frames toward the client
    pub outgoing: mpsc::Sender<Frame>,
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

impl Connection {
    pub fn new(context: Context, outgoing: mpsc::Sender<Frame>) -> Self {
        let conn_id = Uuid::new_v4().as_hyphenated().to_string();

        info!("Client connected id = {conn_id}");

        Self {
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
            exclusive_queues: vec![],
            passively_consumed_queues: HashMap::new(),
            in_flight_contents: HashMap::new(),
            next_confirm_delivery_tag: HashMap::new(),
            outgoing,
        }
    }

    /// Look up exchange sink in exchange manager and returns the exchange sink or a 404 channel error.
    async fn find_exchange(&self, channel: Channel, exchange_name: &str) -> Result<ExchangeCommandSink> {
        match em::get_exchange_sink(
            &self.em,
            em::GetExchangeSinkQuery {
                exchange_name: exchange_name.to_string(),
            },
        )
        .await
        {
            Some(sink) => Ok(sink),
            None => channel_error(
                channel,
                frame::BASIC_PUBLISH,
                ChannelError::NotFound,
                &format!("Exchange {} not found", &exchange_name),
            ),
        }
    }

    pub async fn handle_connection_close(&mut self) -> Result<()> {
        // TODO should be here just to close all channels, not repeating the channel close logic
        // Most of the time we have all channels closed at this point, but what if the connection
        // has been cut and client didn't have a chance to close everything properly?
        for (channel, cq) in self.consumed_queues.drain() {
            let cmd = qm::QueueCancelConsume {
                channel,
                queue_name: cq.queue_name.clone(),
                consumer_tag: cq.consumer_tag.clone(),
            };

            logerr!(qm::cancel_consume(&self.qm, cmd).await);
        }

        for qs in &self.exclusive_queues {
            qm::queue_deleted(
                &self.qm,
                qm::QueueDeletedEvent {
                    queue: qs.queue_name.clone(),
                },
            )
            .await
            .unwrap();
        }

        // TODO cleanup, like close all channels, delete temporal queues, etc
        Ok(())
    }

    pub async fn handle_channel_close(&mut self, channel: Channel) -> Result<()> {
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

        // Cancel passive consumers registered because of a Basic.Get
        if let Some(pq) = self.passively_consumed_queues.remove(&channel) {
            pq.queue_sink
                .send(queue_handler::QueueCommand::PassiveCancelConsume(
                    queue_handler::PassiveCancelConsumeCmd {
                        conn_id: self.id.clone(),
                        channel,
                    },
                ))
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
    pub async fn handle_error(&mut self, err: RuntimeError) -> Result<()> {
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
