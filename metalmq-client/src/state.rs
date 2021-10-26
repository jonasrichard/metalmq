//! `state` module represents the client state machine which handles incoming
//! commands (from client api side) and incoming AMQP frames from network/server
//! side.
//!
//! So everything which comes from the server or goes to the server is an
//! AMQP frame or `MethodFrame`, content etc. Everything which talks to the client
//! api it is a typed struct.

use crate::channel_api::{ConsumerSignal, Message};
use crate::model::ChannelNumber;
use anyhow::Result;
use log::{debug, info};
use metalmq_codec::codec::Frame;
use metalmq_codec::frame::{self, AMQPFrame};
use std::collections::HashMap;
use std::fmt;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
enum Phase {
    Uninitialized,
    Connected,
    Authenticated,
    //    Closing
}

/// A content being delivered by content frames, building step by step.
#[derive(Debug)]
struct DeliveredContent {
    channel: u16,
    consumer_tag: String,
    delivery_tag: u64,
    exchange_name: String,
    routing_key: String,
    body_size: Option<u64>,
    body: Option<Vec<u8>>,
}

/*
 * Consumer handling.
 *
 * A consumer process starts with a basic consume message to the server with a consumer tag.
 * If there is no error, the server starts delivering messages to the same consumer.
 * For that reason we start a new thread, so the delivered messages can be handled independently
 * from the other method frames.
 *
 * TODO If consumer sends a basic cancel, or the channel is closed because of an exception,
 * or if connection is closed because of any reason, we need to unblock the client; so we
 * need to send a message to the consumer process, denoting that the consuming is over.
 *
 * This means that we need to know about the mpsc channel in which we deliver the messages.
 * Moreover, from now on, we are not delivering just messages but control signals as well.
 * So messages look like:
 *   Delivered(Message, CTag, DeliveryTag,...)
 *   ConsumeCancelled
 *   ChannelClosed
 *   ConnectionClosed
 */

// TODO basic consume subscribe to a queue but when messages are delivered we get only the exchange
// name
pub(crate) struct ClientState {
    state: Phase,
    username: String,
    password: String,
    virtual_host: String,
    pub(crate) consumers: HashMap<ChannelNumber, mpsc::UnboundedSender<ConsumerSignal>>,
    pub(self) in_delivery: HashMap<ChannelNumber, DeliveredContent>,
    outgoing: mpsc::Sender<Frame>,
    /// The last delivery tag we sent out per channel.
    ack_sent: HashMap<ChannelNumber, u64>,
    connected: Option<oneshot::Sender<()>>,
}

impl fmt::Debug for ClientState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ClientState {{ state={:?}, username={}, password={} }}",
            &self.state, &self.username, &self.password
        )
    }
}

pub(crate) fn new(outgoing: mpsc::Sender<Frame>) -> ClientState {
    ClientState {
        state: Phase::Uninitialized,
        username: "".to_owned(),
        password: "".to_owned(),
        virtual_host: "/".to_owned(),
        consumers: HashMap::new(),
        in_delivery: HashMap::new(),
        outgoing,
        ack_sent: HashMap::new(),
        connected: None,
    }
}

impl ClientState {
    pub(crate) async fn start(
        &mut self,
        username: String,
        password: String,
        virtual_host: String,
        connected: oneshot::Sender<()>,
    ) -> Result<()> {
        self.username = username;
        self.password = password;
        self.virtual_host = virtual_host;
        self.connected = Some(connected);

        // FIXME we need to give back the result of .await
        self.outgoing.send(Frame::Frame(AMQPFrame::Header)).await.unwrap();

        Ok(())
    }

    pub(crate) async fn connection_start(&mut self, args: frame::ConnectionStartArgs) -> Result<()> {
        info!("Server supported mechanisms: {}", args.mechanisms);

        let mut caps = frame::FieldTable::new();

        caps.insert(
            "authentication_failure_close".to_string(),
            frame::AMQPFieldValue::Bool(true),
        );

        //caps.insert("basic.nack".to_string(), AMQPFieldValue::Bool(true));
        //caps.insert("connection.blocked".to_string(), AMQPFieldValue::Bool(true));
        //caps.insert("consumer_cancel_notify".to_string(), AMQPFieldValue::Bool(true));
        //caps.insert("publisher_confirms".to_string(), AMQPFieldValue::Bool(true));

        self.outgoing
            .send(Frame::Frame(frame::connection_start_ok(
                &self.username,
                &self.password,
                caps,
            )))
            .await
            .unwrap();

        Ok(())
    }

    pub(crate) async fn connection_start_ok(&mut self, args: frame::ConnectionStartOkArgs) -> Result<()> {
        self.state = Phase::Connected;

        Ok(())
    }

    pub(crate) async fn connection_tune(&mut self, _args: frame::ConnectionTuneArgs) -> Result<()> {
        self.state = Phase::Authenticated;

        self.outgoing
            .send(Frame::Frame(frame::connection_tune_ok(0)))
            .await
            .unwrap();
        self.outgoing
            .send(Frame::Frame(frame::connection_open(0, &self.virtual_host)))
            .await
            .unwrap();

        Ok(())
    }

    pub(crate) async fn connection_tune_ok(&mut self, _args: frame::ConnectionTuneOkArgs) -> Result<()> {
        Ok(())
    }

    pub(crate) async fn connection_open(&mut self, args: frame::ConnectionOpenArgs) -> Result<()> {
        Ok(())
    }

    pub(crate) async fn connection_open_ok(&mut self) -> Result<()> {
        let mut conn_tx = None;

        std::mem::swap(&mut conn_tx, &mut self.connected);
        conn_tx.unwrap().send(()).unwrap();

        Ok(())
    }

    pub(crate) async fn connection_close(&mut self, _args: frame::ConnectionCloseArgs) -> Result<()> {
        self.outgoing
            .send(Frame::Frame(frame::connection_close(0, 200, "Normal close", 0, 0)))
            .await?;

        Ok(())
    }

    pub(crate) async fn connection_close_ok(&mut self) -> Result<()> {
        for consumer in &self.consumers {
            consumer.1.send(ConsumerSignal::ConnectionClosed)?;
        }

        Ok(())
    }

    pub(crate) async fn handle_connection_close(&mut self, _args: frame::ConnectionCloseArgs) -> Result<()> {
        // TODO close resources, server is about to close connection
        Ok(())
    }

    pub(crate) async fn channel_open(&mut self, channel: ChannelNumber) -> Result<()> {
        self.outgoing.send(Frame::Frame(frame::channel_open(channel))).await?;

        Ok(())
    }

    pub(crate) async fn channel_open_ok(&mut self, _channel: ChannelNumber) -> Result<()> {
        Ok(())
    }

    pub(crate) async fn channel_close(&mut self, channel: ChannelNumber, args: frame::ChannelCloseArgs) -> Result<()> {
        self.outgoing
            .send(Frame::Frame(frame::channel_close(
                channel,
                args.code,
                &args.text,
                args.class_id,
                args.method_id,
            )))
            .await?;

        Ok(())
    }

    pub(crate) async fn channel_close_ok(&mut self, channel: ChannelNumber) -> Result<()> {
        if let Some(sink) = self.consumers.remove(&channel) {
            sink.send(ConsumerSignal::ChannelClosed)?;
            drop(sink);
        }

        Ok(())
    }

    pub(crate) async fn handle_channel_close(
        &mut self,
        channel: ChannelNumber,
        _args: frame::ChannelCloseArgs,
    ) -> Result<()> {
        if let Some(sink) = self.consumers.remove(&channel) {
            drop(sink);
        }

        Ok(())
    }

    pub(crate) async fn exchange_declare(
        &mut self,
        channel: ChannelNumber,
        args: frame::ExchangeDeclareArgs,
    ) -> Result<()> {
        self.outgoing
            .send(Frame::Frame(frame::exchange_declare(
                channel,
                &args.exchange_name,
                &args.exchange_type,
                Some(args.flags),
            )))
            .await?;

        Ok(())
    }

    pub(crate) async fn exchange_declare_ok(&mut self) -> Result<()> {
        Ok(())
    }

    pub(crate) async fn exchange_delete(
        &mut self,
        channel: ChannelNumber,
        args: frame::ExchangeDeleteArgs,
    ) -> Result<()> {
        self.outgoing
            .send(Frame::Frame(frame::exchange_delete(
                channel,
                &args.exchange_name,
                Some(args.flags),
            )))
            .await?;

        Ok(())
    }

    pub(crate) async fn exchange_delete_ok(&mut self) -> Result<()> {
        Ok(())
    }

    pub(crate) async fn queue_declare(&mut self, channel: ChannelNumber, args: frame::QueueDeclareArgs) -> Result<()> {
        self.outgoing
            .send(Frame::Frame(frame::queue_declare(
                channel,
                &args.name,
                Some(args.flags),
            )))
            .await?;

        Ok(())
    }

    pub(crate) async fn queue_declare_ok(&mut self, _args: frame::QueueDeclareOkArgs) -> Result<()> {
        Ok(())
    }

    pub(crate) async fn queue_bind(&mut self, channel: ChannelNumber, args: frame::QueueBindArgs) -> Result<()> {
        self.outgoing
            .send(Frame::Frame(frame::queue_bind(
                channel,
                &args.queue_name,
                &args.exchange_name,
                &args.routing_key,
            )))
            .await?;

        Ok(())
    }

    pub(crate) async fn queue_bind_ok(&mut self) -> Result<()> {
        Ok(())
    }

    pub(crate) async fn queue_unbind(&mut self, channel: ChannelNumber, args: frame::QueueUnbindArgs) -> Result<()> {
        self.outgoing
            .send(Frame::Frame(frame::queue_unbind(
                channel,
                &args.queue_name,
                &args.exchange_name,
                &args.routing_key,
            )))
            .await?;

        Ok(())
    }

    pub(crate) async fn queue_unbind_ok(&mut self) -> Result<()> {
        Ok(())
    }

    pub(crate) async fn queue_delete(&mut self, channel: ChannelNumber, args: frame::QueueDeleteArgs) -> Result<()> {
        // TODO what happens if I am consuming that queue?
        self.outgoing
            .send(Frame::Frame(frame::queue_delete(
                channel,
                &args.queue_name,
                Some(args.flags),
            )))
            .await?;

        Ok(())
    }

    pub(crate) async fn queue_delete_ok(
        &mut self,
        channel: ChannelNumber,
        args: frame::QueueDeleteOkArgs,
    ) -> Result<()> {
        Ok(())
    }

    pub(crate) async fn basic_ack(&mut self, channel: ChannelNumber, args: frame::BasicAckArgs) -> Result<()> {
        self.outgoing
            .send(Frame::Frame(frame::basic_ack(
                channel,
                args.delivery_tag,
                args.multiple,
            )))
            .await?;

        if let Some(dt) = self.ack_sent.get_mut(&channel) {
            debug_assert!(*dt < args.delivery_tag);
            *dt = args.delivery_tag;
        } else {
            self.ack_sent.insert(channel, args.delivery_tag);
        }

        Ok(())
    }

    pub(crate) async fn basic_consume(
        &mut self,
        channel: ChannelNumber,
        args: frame::BasicConsumeArgs,
        sink: mpsc::UnboundedSender<ConsumerSignal>,
    ) -> Result<()> {
        self.consumers.insert(channel, sink);

        self.outgoing
            .send(Frame::Frame(frame::basic_consume(
                channel,
                &args.queue,
                &args.consumer_tag,
                Some(args.flags),
            )))
            .await
            .unwrap();

        Ok(())
    }

    pub(crate) async fn basic_consume_ok(&mut self, _args: frame::BasicConsumeOkArgs) -> Result<()> {
        Ok(())
    }

    pub(crate) async fn basic_cancel(&mut self, channel: ChannelNumber, args: frame::BasicCancelArgs) -> Result<()> {
        self.outgoing
            .send(Frame::Frame(frame::basic_cancel(
                channel,
                &args.consumer_tag,
                args.no_wait,
            )))
            .await?;

        Ok(())
    }

    pub(crate) async fn basic_cancel_ok(
        &mut self,
        channel: ChannelNumber,
        args: frame::BasicCancelOkArgs,
    ) -> Result<()> {
        if let Some(consumer_sink) = self.consumers.remove(&channel) {
            consumer_sink.send(ConsumerSignal::Cancelled)?;
        }

        Ok(())
    }

    pub(crate) async fn basic_deliver(&mut self, channel: ChannelNumber, args: frame::BasicDeliverArgs) -> Result<()> {
        let dc = DeliveredContent {
            channel,
            consumer_tag: args.consumer_tag,
            delivery_tag: args.delivery_tag,
            exchange_name: args.exchange_name,
            routing_key: args.routing_key,
            body_size: None,
            body: None,
        };

        self.in_delivery.insert(channel, dc);

        Ok(())
    }

    pub(crate) async fn basic_publish(
        &mut self,
        channel: ChannelNumber,
        args: frame::BasicPublishArgs,
        content: Vec<u8>,
    ) -> Result<()> {
        let fs = vec![
            frame::basic_publish(channel, &args.exchange_name, &args.routing_key),
            AMQPFrame::ContentHeader(frame::content_header(channel, content.len() as u64)),
            AMQPFrame::ContentBody(frame::content_body(channel, content.as_slice())),
        ];

        self.outgoing.send(Frame::Frames(fs)).await.unwrap();

        Ok(())
    }

    pub(crate) async fn content_header(&mut self, ch: frame::ContentHeaderFrame) -> Result<()> {
        if let Some(dc) = self.in_delivery.get_mut(&ch.channel) {
            dc.body_size = Some(ch.body_size);
        }

        // TODO error handling

        Ok(())
    }

    pub(crate) async fn content_body(&mut self, cb: frame::ContentBodyFrame) -> Result<()> {
        if let Some(dc) = self.in_delivery.get(&cb.channel) {
            debug!("Delivered content is {:?} so far", dc);

            if let Some(sink) = self.consumers.get(&dc.channel) {
                let msg = Message {
                    channel: dc.channel,
                    consumer_tag: dc.consumer_tag.clone(),
                    delivery_tag: dc.delivery_tag,
                    length: dc.body_size.unwrap() as usize,
                    body: cb.body,
                };

                // FIXME here if the channel is full, this call yields. The problem is that the
                // consumer is sending back an ack which should be processed by the processor
                // but since the async call yields here, that select is never reached.
                // A solution can be to apply backpressure and in ack mode there shouldn't be
                // more than capacity number of unacked messages.
                sink.send(ConsumerSignal::Delivered(msg)).unwrap();
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metalmq_codec::frame::{self, AMQPFrame};

    #[tokio::test]
    async fn connect_open_sets_virtual_host() {
        let virtual_host = "/".to_owned();
        let (tx, mut rx) = mpsc::channel(1);
        let mut cs = new(tx);
        let args = frame::ConnectionOpenArgs {
            virtual_host,
            insist: false,
        };

        cs.connection_open(args).await.unwrap();

        let outgoing_frame = rx.recv().await.unwrap();

        assert!(matches!(
            outgoing_frame,
            Frame::Frame(AMQPFrame::Method(
                0,
                frame::CONNECTION_OPEN,
                _,
                //frame::MethodFrameArgs::ConnectionOpen(frame::ConnectionOpenArgs {
                //    virtual_host,
                //    insist: false,
                //})
            ))
        ));
    }

    #[tokio::test]
    async fn connection_close_sends_consumer_signal() {
        let (tx, _) = mpsc::channel(1);
        let mut cs = new(tx);
        let (consumer_sink, mut consumer_stream) = mpsc::unbounded_channel();

        cs.consumers.insert(2, consumer_sink);

        cs.connection_close_ok().await.unwrap();

        let signal = consumer_stream.recv().await.unwrap();

        assert!(matches!(signal, ConsumerSignal::ConnectionClosed));
    }

    #[tokio::test]
    async fn channel_close_sends_consumer_signal() {
        let (tx, _) = mpsc::channel(1);
        let mut cs = new(tx);
        let (consumer_sink, mut consumer_stream) = mpsc::unbounded_channel();

        cs.consumers.insert(2, consumer_sink);

        cs.channel_close_ok(2).await.unwrap();

        let signal = consumer_stream.recv().await.unwrap();

        assert!(matches!(signal, ConsumerSignal::ChannelClosed));
    }

    #[tokio::test]
    async fn basic_consume_sends_signal() {
        let ctag = "ctag1".to_owned();
        let (tx, _) = mpsc::channel(1);
        let mut cs = new(tx);
        let args = frame::BasicCancelOkArgs { consumer_tag: ctag };
        let (consumer_sink, mut consumer_stream) = mpsc::unbounded_channel();

        cs.consumers.insert(1, consumer_sink);

        cs.basic_cancel_ok(1, args).await.unwrap();

        let signal = consumer_stream.recv().await.unwrap();

        assert!(matches!(signal, ConsumerSignal::Cancelled));
    }
}
