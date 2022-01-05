// Do we need to expose the messages of a 'process' or hide it in an erlang-style?
use crate::client;
use crate::exchange::handler::ExchangeCommandSink;
use crate::exchange::manager as em;
use crate::queue::handler as queue_handler;
use crate::queue::manager as qm;
use crate::{Context, ErrorScope, Result, RuntimeError};
use log::info;
use metalmq_codec::codec::Frame;
use metalmq_codec::frame::{self, Channel};
use std::collections::HashMap;
use tokio::sync::mpsc;
use uuid::Uuid;

pub mod basic;
pub mod channel;
pub mod connect;
pub mod exchange;
pub mod queue;

#[derive(Debug)]
struct ConsumedQueue {
    channel: Channel,
    queue_name: String,
    consumer_tag: String,
    queue_sink: queue_handler::QueueCommandSink,
}

/// All the transient data of a connection are stored here.
pub struct Connection {
    /// Unique ID of the connection.
    id: String,
    qm: qm::QueueManagerSink,
    em: em::ExchangeManagerSink,
    /// Opened channels by this connection.
    open_channels: Vec<Channel>,
    /// Declared exchanges by this connection.
    exchanges: HashMap<String, ExchangeCommandSink>,
    /// Exchanges which declared by this channel as auto-delete
    auto_delete_exchanges: Vec<String>,
    /// Consumed queues by this connection
    consumed_queues: Vec<ConsumedQueue>,
    /// Incoming messages come in different messages, we need to collect their properties
    in_flight_contents: HashMap<Channel, PublishedContent>,
    /// Sink for AMQP frames toward the client
    outgoing: mpsc::Sender<Frame>,
}

#[derive(Debug, Default)]
struct PublishedContent {
    channel: Channel,
    exchange: String,
    routing_key: String,
    mandatory: bool,
    immediate: bool,
    content_header: frame::ContentHeaderFrame,
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
    let conn_id = Uuid::new_v4().to_hyphenated().to_string();

    info!("Client connected id = {}", conn_id);

    Connection {
        id: conn_id,
        qm: context.queue_manager,
        em: context.exchange_manager,
        open_channels: vec![],
        exchanges: HashMap::new(),
        auto_delete_exchanges: vec![],
        consumed_queues: vec![],
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

    async fn handle_connection_close(&self) -> Result<()> {
        // TODO cleanup, like close all channels, delete temporal queues, etc
        Ok(())
    }

    async fn handle_channel_close(&mut self, channel: frame::Channel) -> Result<()> {
        //let consumes_to_cancel = self.consumed_queues.drain_filter(|cq| cq.channel == channel).collect();
        let mut consumes_to_cancel = vec![];

        loop {
            match self.consumed_queues.iter().position(|cq| cq.channel == channel) {
                None => break,
                Some(p) => {
                    let cq = self.consumed_queues.remove(p);
                    consumes_to_cancel.push(cq);
                }
            }
        }

        for cq in consumes_to_cancel {
            self.basic_cancel(
                channel,
                frame::BasicCancelArgs {
                    consumer_tag: cq.consumer_tag.clone(),
                    no_wait: false,
                },
            )
            .await?;
        }
        // TODO cleanup, like cancel all consumers, etc.
        Ok(())
    }

    async fn handle_error(&mut self, err: RuntimeError) -> Result<()> {
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
}
