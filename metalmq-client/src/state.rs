//! `state` module represents the client state machine which handles incoming
//! commands (from client api side) and incoming AMQP frames from network/server
//! side.
//!
//! So everything which comes from the server or goes to the server is an
//! AMQP frame or `MethodFrame`, content etc. Everything which talks to the client
//! api it is a typed struct.

use crate::{
    client_api::ConnectionSink,
    consumer::ConsumerSignal,
    message::{self, Message},
    model::ChannelNumber,
    processor::{OutgoingFrame, WaitFor},
    EventSignal,
};
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

#[derive(Debug)]
enum DeliveryMethod {
    BasicDeliver {
        consumer_tag: String,
        delivery_tag: u64,
        redelivered: bool,
        exchange: String,
        routing_key: String,
    },
    BasicReturn {
        reply_code: u16,
        reply_text: String,
        exchange: String,
        routing_key: String,
    },
}

/// A content being delivered by content frames, building step by step.
#[derive(Debug)]
struct DeliveredContent {
    channel: u16,
    method: DeliveryMethod,
    message: Message,
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
    outgoing: mpsc::Sender<OutgoingFrame>,
    /// The last delivery tag we sent out per channel.
    ack_sent: HashMap<ChannelNumber, u64>,
    connected: Option<oneshot::Sender<()>>,
    event_sink: ConnectionSink,
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

pub(crate) fn new(outgoing: mpsc::Sender<OutgoingFrame>, conn_evt_tx: ConnectionSink) -> ClientState {
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
        event_sink: conn_evt_tx,
    }
}

async fn send_out(
    out: &mpsc::Sender<OutgoingFrame>,
    frame: Frame,
) -> std::result::Result<(), mpsc::error::SendError<OutgoingFrame>> {
    out.send(OutgoingFrame { frame, written: None }).await
}

impl ClientState {
    /// Starts the client sending connection start and tune and open frames.
    /// Connects to the virtual host and when connection-open-ok message comes
    /// back it notifies the `connected` channel.
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
        send_out(&self.outgoing, Frame::Frame(AMQPFrame::Header)).await.unwrap();

        Ok(())
    }

    pub(crate) async fn connection_start(&mut self, args: frame::ConnectionStartArgs) -> Result<()> {
        info!("Server supported mechanisms: {}", args.mechanisms);

        //caps.insert("basic.nack".to_string(), AMQPFieldValue::Bool(true));
        //caps.insert("connection.blocked".to_string(), AMQPFieldValue::Bool(true));
        //caps.insert("consumer_cancel_notify".to_string(), AMQPFieldValue::Bool(true));
        //caps.insert("publisher_confirms".to_string(), AMQPFieldValue::Bool(true));

        send_out(
            &self.outgoing,
            Frame::Frame(frame::ConnectionStartOkArgs::new(&self.username, &self.password).frame()),
        )
        .await
        .unwrap();

        Ok(())
    }

    //pub(crate) async fn connection_start_ok(&mut self, args: frame::ConnectionStartOkArgs) -> Result<()> {
    //    self.state = Phase::Connected;

    //    Ok(())
    //}

    pub(crate) async fn connection_tune(&mut self, _args: frame::ConnectionTuneArgs) -> Result<()> {
        self.state = Phase::Authenticated;

        send_out(&self.outgoing, Frame::Frame(frame::connection_tune_ok()))
            .await
            .unwrap();

        send_out(
            &self.outgoing,
            Frame::Frame(
                frame::ConnectionOpenArgs::default()
                    .virtual_host(&self.virtual_host)
                    .frame(),
            ),
        )
        .await
        .unwrap();

        Ok(())
    }

    //pub(crate) async fn connection_tune_ok(&mut self, _args: frame::ConnectionTuneOkArgs) -> Result<()> {
    //    Ok(())
    //}

    //pub(crate) async fn connection_open(&mut self, args: frame::ConnectionOpenArgs) -> Result<()> {
    //    Ok(())
    //}

    pub(crate) async fn connection_open_ok(&mut self) -> Result<()> {
        let mut conn_tx = None;

        std::mem::swap(&mut conn_tx, &mut self.connected);
        conn_tx.unwrap().send(()).unwrap();

        Ok(())
    }

    pub(crate) async fn connection_close(&mut self, _args: frame::ConnectionCloseArgs) -> Result<()> {
        send_out(
            &self.outgoing,
            Frame::Frame(frame::connection_close(200, "Normal close", 0)),
        )
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
        self.event_sink.send(EventSignal::ConnectionClose).unwrap();

        // TODO close resources, server is about to close connection
        Ok(())
    }

    pub(crate) async fn channel_open(&mut self, channel: ChannelNumber) -> Result<()> {
        send_out(&self.outgoing, Frame::Frame(frame::channel_open(channel))).await?;

        Ok(())
    }

    pub(crate) async fn channel_open_ok(&mut self, _channel: ChannelNumber) -> Result<()> {
        Ok(())
    }

    pub(crate) async fn channel_close(&mut self, channel: ChannelNumber, args: frame::ChannelCloseArgs) -> Result<()> {
        send_out(&self.outgoing, Frame::Frame(args.frame(channel))).await?;

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

        self.event_sink.send(EventSignal::ChannelClose).unwrap();

        Ok(())
    }

    pub(crate) async fn exchange_declare(
        &mut self,
        channel: ChannelNumber,
        args: frame::ExchangeDeclareArgs,
    ) -> Result<()> {
        send_out(&self.outgoing, Frame::Frame(args.frame(channel))).await?;

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
        send_out(&self.outgoing, Frame::Frame(args.frame(channel))).await?;

        Ok(())
    }

    pub(crate) async fn exchange_delete_ok(&mut self) -> Result<()> {
        Ok(())
    }

    pub(crate) async fn queue_declare(&mut self, channel: ChannelNumber, args: frame::QueueDeclareArgs) -> Result<()> {
        send_out(&self.outgoing, Frame::Frame(args.frame(channel))).await?;

        Ok(())
    }

    pub(crate) async fn queue_declare_ok(&mut self, _args: frame::QueueDeclareOkArgs) -> Result<()> {
        Ok(())
    }

    pub(crate) async fn queue_bind(&mut self, channel: ChannelNumber, args: frame::QueueBindArgs) -> Result<()> {
        send_out(&self.outgoing, Frame::Frame(args.frame(channel))).await?;

        Ok(())
    }

    pub(crate) async fn queue_bind_ok(&mut self) -> Result<()> {
        Ok(())
    }

    pub(crate) async fn queue_unbind(&mut self, channel: ChannelNumber, args: frame::QueueUnbindArgs) -> Result<()> {
        send_out(&self.outgoing, Frame::Frame(args.frame(channel))).await?;

        Ok(())
    }

    pub(crate) async fn queue_unbind_ok(&mut self) -> Result<()> {
        Ok(())
    }

    pub(crate) async fn queue_purge(&mut self, channel: ChannelNumber, args: frame::QueuePurgeArgs) -> Result<()> {
        send_out(&self.outgoing, Frame::Frame(args.frame(channel))).await?;

        Ok(())
    }

    pub(crate) async fn queue_delete(&mut self, channel: ChannelNumber, args: frame::QueueDeleteArgs) -> Result<()> {
        // TODO what happens if I am consuming that queue?
        send_out(&self.outgoing, Frame::Frame(args.frame(channel))).await?;

        Ok(())
    }

    pub(crate) async fn queue_delete_ok(
        &mut self,
        channel: ChannelNumber,
        args: frame::QueueDeleteOkArgs,
    ) -> Result<()> {
        Ok(())
    }

    pub(crate) async fn basic_ack(
        &mut self,
        channel: ChannelNumber,
        args: frame::BasicAckArgs,
        wait_for: WaitFor,
    ) -> Result<()> {
        let delivery_tag = args.delivery_tag;

        send_out(&self.outgoing, Frame::Frame(args.frame(channel))).await?;

        if let WaitFor::SentOut(tx) = wait_for {
            tx.send(Ok(())).unwrap();
        }

        if let Some(dt) = self.ack_sent.get_mut(&channel) {
            debug_assert!(*dt < delivery_tag);
            *dt = delivery_tag;
        } else {
            self.ack_sent.insert(channel, delivery_tag);
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

        send_out(&self.outgoing, Frame::Frame(args.frame(channel)))
            .await
            .unwrap();

        Ok(())
    }

    pub(crate) async fn basic_consume_ok(&mut self, _args: frame::BasicConsumeOkArgs) -> Result<()> {
        Ok(())
    }

    pub(crate) async fn basic_cancel(&mut self, channel: ChannelNumber, args: frame::BasicCancelArgs) -> Result<()> {
        send_out(&self.outgoing, Frame::Frame(args.frame(channel))).await?;

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
        let message = Message {
            channel,
            body: vec![],
            properties: message::MessageProperties::default(),
            delivery_info: Some(message::DeliveryInfo {
                consumer_tag: args.consumer_tag.clone(),
                delivery_tag: args.delivery_tag.clone(),
                routing_key: args.routing_key.clone(),
            }),
        };

        let dc = DeliveredContent {
            channel,
            method: DeliveryMethod::BasicDeliver {
                consumer_tag: args.consumer_tag,
                delivery_tag: args.delivery_tag,
                redelivered: args.redelivered,
                exchange: args.exchange_name,
                routing_key: args.routing_key,
            },
            message: Message::default(),
        };

        self.in_delivery.insert(channel, dc);

        Ok(())
    }

    pub(crate) async fn basic_publish(
        &mut self,
        channel: ChannelNumber,
        args: frame::BasicPublishArgs,
        message: Message,
    ) -> Result<()> {
        let (mut ch, cb) = message::to_content_frames(message);
        ch.class_id = (frame::BASIC_PUBLISH >> 16) as u16;

        let fs = vec![args.frame(channel), ch.frame(), cb.frame()];

        send_out(&self.outgoing, Frame::Frames(fs)).await.unwrap();

        Ok(())
    }

    pub(crate) async fn basic_return(&mut self, channel: ChannelNumber, args: frame::BasicReturnArgs) -> Result<()> {
        let message = Message {
            channel,
            body: vec![],
            properties: message::MessageProperties::default(),
            delivery_info: Some(message::DeliveryInfo {
                consumer_tag: "".to_string(),
                delivery_tag: 0u64,
                routing_key: args.routing_key.clone(),
                // FIXME exchange??? we need an enum here, too
            }),
        };

        let dc = DeliveredContent {
            channel,
            method: DeliveryMethod::BasicReturn {
                reply_code: args.reply_code,
                reply_text: args.reply_text,
                exchange: args.exchange_name,
                routing_key: args.routing_key,
            },
            message,
        };

        self.in_delivery.insert(channel, dc);

        Ok(())
    }

    pub(crate) async fn confirm_select(&mut self, channel: ChannelNumber) -> Result<()> {
        send_out(&self.outgoing, Frame::Frame(frame::confirm_select(channel)))
            .await
            .unwrap();

        Ok(())
    }

    pub(crate) async fn content_header(&mut self, ch: frame::ContentHeaderFrame) -> Result<()> {
        if let Some(dc) = self.in_delivery.get_mut(&ch.channel) {
            dc.message.properties = ch.into();
        }

        // TODO error handling

        Ok(())
    }

    pub(crate) async fn content_body(&mut self, cb: frame::ContentBodyFrame) -> Result<()> {
        if let Some(dc) = self.in_delivery.remove(&cb.channel) {
            debug!("Delivered content is {:?} so far", dc);

            if let Some(sink) = self.consumers.get(&dc.channel) {
                match dc.method {
                    DeliveryMethod::BasicDeliver {
                        consumer_tag,
                        delivery_tag,
                        ..
                    } => {
                        let msg = Message {
                            channel: dc.channel,
                            consumer_tag,
                            delivery_tag,
                            length: dc.body_size.unwrap() as usize,
                            body: cb.body,
                        };
                        sink.send(ConsumerSignal::Delivered(msg)).unwrap();
                    }
                    DeliveryMethod::BasicReturn {
                        reply_code,
                        reply_text,
                        exchange,
                        routing_key,
                    } => self
                        .event_sink
                        .send(EventSignal::BasicReturn {
                            channel: dc.channel,
                            args: frame::BasicReturnArgs {
                                reply_code,
                                reply_text,
                                exchange_name: exchange,
                                routing_key,
                            },
                        })
                        .unwrap(),
                };

                // FIXME here if the channel is full, this call yields. The problem is that the
                // consumer is sending back an ack which should be processed by the processor
                // but since the async call yields here, that select is never reached.
                // A solution can be to apply backpressure and in ack mode there shouldn't be
                // more than capacity number of unacked messages.
            }
        }

        Ok(())
    }

    pub(crate) async fn on_basic_ack(&mut self, channel: ChannelNumber, args: frame::BasicAckArgs) -> Result<()> {
        use crate::client_api::EventSignal;

        self.event_sink
            .send(EventSignal::BasicAck {
                channel,
                delivery_tag: args.delivery_tag,
                multiple: args.multiple,
            })
            .unwrap();

        Ok(())
    }

    pub(crate) async fn on_basic_return(&mut self, channel: ChannelNumber, args: frame::BasicReturnArgs) -> Result<()> {
        use crate::client_api::EventSignal;

        self.event_sink
            .send(EventSignal::BasicReturn { channel, args })
            .unwrap();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metalmq_codec::frame;

    #[tokio::test]
    async fn connect_open_sets_virtual_host() {
        let virtual_host = "/".to_owned();
        let (conn_tx, conn_rx) = mpsc::unbounded_channel();
        let (tx, mut rx) = mpsc::channel(2);
        let mut cs = new(tx, conn_tx);
        let args = frame::ConnectionTuneArgs {
            channel_max: 2047,
            frame_max: 65535,
            heartbeat: 60,
        };

        cs.connection_tune(args).await.unwrap();

        let outgoing_tune_ok_frame = rx.recv().await.unwrap();
        let OutgoingFrame { frame: open_frame, .. } = rx.recv().await.unwrap();

        //assert!(matches!(
        //    open_frame,
        //    Frame::Frame(AMQPFrame::Method(
        //        0,
        //        frame::CONNECTION_OPEN,
        //        _,
        //        //frame::MethodFrameArgs::ConnectionOpen(frame::ConnectionOpenArgs {
        //        //    virtual_host,
        //        //    insist: false,
        //        //})
        //    )))
        //));
    }

    #[tokio::test]
    async fn connection_close_sends_consumer_signal() {
        let (conn_tx, conn_rx) = mpsc::unbounded_channel();
        let (tx, _) = mpsc::channel(1);
        let mut cs = new(tx, conn_tx);
        let (consumer_sink, mut consumer_stream) = mpsc::unbounded_channel();

        cs.consumers.insert(2, consumer_sink);

        cs.connection_close_ok().await.unwrap();

        let signal = consumer_stream.recv().await.unwrap();

        assert!(matches!(signal, ConsumerSignal::ConnectionClosed));
    }

    #[tokio::test]
    async fn channel_close_sends_consumer_signal() {
        let (conn_tx, conn_rx) = mpsc::unbounded_channel();
        let (tx, _) = mpsc::channel(1);
        let mut cs = new(tx, conn_tx);
        let (consumer_sink, mut consumer_stream) = mpsc::unbounded_channel();

        cs.consumers.insert(2, consumer_sink);

        cs.channel_close_ok(2).await.unwrap();

        let signal = consumer_stream.recv().await.unwrap();

        assert!(matches!(signal, ConsumerSignal::ChannelClosed));
    }

    #[tokio::test]
    async fn basic_consume_sends_signal() {
        let ctag = "ctag1".to_owned();
        let (conn_tx, conn_rx) = mpsc::unbounded_channel();
        let (tx, _) = mpsc::channel(1);
        let mut cs = new(tx, conn_tx);
        let args = frame::BasicCancelOkArgs { consumer_tag: ctag };
        let (consumer_sink, mut consumer_stream) = mpsc::unbounded_channel();

        cs.consumers.insert(1, consumer_sink);

        cs.basic_cancel_ok(1, args).await.unwrap();

        let signal = consumer_stream.recv().await.unwrap();

        assert!(matches!(signal, ConsumerSignal::Cancelled));
    }
}
