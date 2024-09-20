use std::collections::HashMap;

use log::info;
use metalmq_codec::codec::Frame;
use tokio::{sync::mpsc, task::JoinHandle};
use uuid::Uuid;

use crate::{client::channel::types::Command, exchange, queue, Context, Result};

/// Status of the connection.
#[derive(Debug)]
pub enum ConnectionState {
    /// The connection data structure is created and initialized.
    Initiated,
    /// Client sent the AMQP header, so it is connected.
    Connected,
    /// Client successfully authenticated itself.
    Authenticated,
    /// Client initiated to close the connection.
    ClosingByClient,
    /// Server (probably because of an error condition) initiated to close the connection.
    ClosingByServer,
    /// The connection is regarded to be closed, so cleanup can go on on server side.
    Closed,
}

/// Exclusive queue declared by the connection
#[derive(Debug)]
pub struct ExclusiveQueue {
    pub queue_name: String,
}

/// Represent a client connection to a server.
pub struct Connection {
    /// Unique ID of the connection.
    pub id: String,
    /// The current state of the connection.
    ///
    /// Helps in closing phase mainly.
    pub status: ConnectionState,
    /// The highest channel number, 0 if there is no limit.
    pub channel_max: u16,
    /// The maximal size of frames the client can accept.
    pub frame_max: usize,
    /// How frequently the server sends heartbeat (at most).
    pub heartbeat_interval: Option<std::time::Duration>,
    /// Exchanges which declared by this channel as auto-delete
    pub auto_delete_exchanges: Vec<String>,
    /// Exclusive queues created by the connection.
    pub exclusive_queues: Vec<ExclusiveQueue>,
    /// Sender part of the queue command channel to the queue manager
    pub qm: queue::manager::QueueManagerSink,
    /// Sender part of the exchange command channel to the exchange manager
    pub em: exchange::manager::ExchangeManagerSink,
    /// [`JoinHandle`] of the channels.
    ///
    /// To be sure that a channel is stopped one need to `await` on that handler.
    pub channel_handlers: HashMap<u16, JoinHandle<Result<()>>>,
    /// The incoming frame channel of a [`crate::client::channel::types::Channel`].
    ///
    /// Through this channel the AMQP channel gets the incoming frames, so it can handle them.
    pub channel_receivers: HashMap<u16, mpsc::Sender<Command>>,
    /// Sink for AMQP frames toward the client
    ///
    /// This is cloned by each channel in order that they can send back response frames.
    pub outgoing: mpsc::Sender<Frame>,
}

impl Connection {
    /// Creates a new connection with the queue and exchange managers and the outgoing frame
    /// channel.
    pub fn new(context: Context, outgoing: mpsc::Sender<Frame>) -> Self {
        let conn_id = Uuid::new_v4().as_hyphenated().to_string();

        info!("Client connected id = {conn_id}");

        Self {
            id: conn_id,
            status: ConnectionState::Initiated,
            qm: context.queue_manager,
            em: context.exchange_manager,
            channel_max: 2047,
            frame_max: 131_072,
            heartbeat_interval: None,
            auto_delete_exchanges: vec![],
            exclusive_queues: vec![],
            channel_handlers: HashMap::new(),
            channel_receivers: HashMap::new(),
            outgoing,
        }
    }

    pub async fn close(&mut self) -> Result<()> {
        info!("Cleanup connection {}", self.id);

        for (channel, ch_tx) in &self.channel_receivers {
            // drop channel channel in order to stop it
            let _ = *ch_tx;

            if let Some(jh) = self.channel_handlers.remove(&channel) {
                //jh.abort();

                //let x = jh.await;

                //dbg!(x);
            }
        }

        //for (channel, cq) in &self.consumed_queues {
        //    debug!(
        //        "Cancel consumer channel: {} queue: {} consumer tag: {}",
        //        channel, cq.queue_name, cq.consumer_tag
        //    );

        //    let cmd = QueueCancelConsume {
        //        channel: *channel,
        //        queue_name: cq.queue_name.clone(),
        //        consumer_tag: cq.consumer_tag.clone(),
        //    };

        //    logerr!(qm::cancel_consume(&self.qm, cmd).await);
        //}

        Ok(())
    }

    /// Forcedly close the connection without sending frames out.
    ///
    /// Usually when there is an error caused by client (accessing not existing resources, or
    /// misusing exchanges or queues), the server closes the connection but it is not expected to
    /// be sent out consume cancellations, channel closing frames, etc.
    pub async fn forced_close(&mut self) -> Result<()> {
        for (_channel, ch_tx) in self.channel_receivers.drain() {
            drop(ch_tx);
        }

        for (_channel, jh) in self.channel_handlers.drain() {
            let _ = jh.await;
            //jh.abort();
        }

        Ok(())
    }

    pub async fn close_channel(&mut self, channel: u16) -> Result<()> {
        if let Some(ch_tx) = self.channel_receivers.remove(&channel) {
            drop(ch_tx);
        }

        if let Some(jh) = self.channel_handlers.remove(&channel) {
            jh.await;
        }

        Ok(())
    }

    // Handle a runtime error a connection or a channel error. At first it sends the error frame
    // and then handle the closing of a channel or connection depending what kind of exception
    // happened.
    //
    // This function just sends out the error frame and return with `Err` if it is a connection
    // error, or it returns with `Ok` if it is a channel error. This is handy if we want to handle
    // the output with a `?` operator and we want to die in case of a connection error (aka we
    // want to propagate the error to the client handler).
    //async fn handle_error(&mut self, err: RuntimeError) -> Result<()> {
    //    trace!("Handling error {:?}", err);

    //    self.send_frame(runtime_error_to_frame(&err)).await?;

    //    match err.scope {
    //        ErrorScope::Connection => {
    //            self.close().await?;

    //            Err(Box::new(err))
    //        }
    //        ErrorScope::Channel => {
    //            self.close_channel(err.channel).await?;

    //            Ok(())
    //        }
    //    }
    //}
}
