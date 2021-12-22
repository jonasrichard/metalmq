// Do we need to expose the messages of a 'process' or hide it in an erlang-style?
use crate::client;
use crate::client::conn::SendFrame;
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
use tokio::sync::oneshot;
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
    // FIXME
    // Why we cannot have here the sender end of the socket? Sometimes, it seems that it is a big
    // pain that we don't know if we have written frames on the wire or haven't. If there is no
    // blocking coming from channel send, we can put here the outgoing as socket write end.
    /// Sink for AMQP frames toward the client
    outgoing: mpsc::Sender<SendFrame>,
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

pub fn new(context: Context, outgoing: mpsc::Sender<SendFrame>) -> Connection {
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
        self.outgoing.send(SendFrame::Async(f)).await?;

        Ok(())
    }

    /// Send frame out to client asynchronously.
    pub async fn send_sync_frame(&self, f: Frame) -> Result<()> {
        let (tx, rx) = oneshot::channel();

        self.outgoing.send(SendFrame::Sync(f, tx)).await?;

        rx.await?;

        Ok(())
    }

    async fn handle_channel_close(&self, channel: frame::Channel) -> Result<()> {
        Ok(())
    }

    async fn handle_error(&self, err: RuntimeError) -> Result<()> {
        self.send_frame(client::runtime_error_to_frame(&err)).await?;

        match err.scope {
            ErrorScope::Connection => Err(Box::new(err)),
            ErrorScope::Channel => {
                self.handle_channel_close(err.channel).await?;

                Ok(())
            }
        }
    }
}
