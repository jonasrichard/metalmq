//! `client_sm` module represents the client state machine which handles incoming
//! commands (from client api side) and incoming AMQP frames from network/server
//! side.
//!
//! So everything which comes from the server or goes to the server is an
//! AMQP frame or `MethodFrame`, content etc. Everything which talks to the client
//! api it is a typed struct.

use crate::{Message, MessageSink};
use anyhow::Result;
use log::{debug, info};
use metalmq_codec::codec::Frame;
use metalmq_codec::frame::{self, AMQPFrame, Channel};
use std::collections::HashMap;
use std::fmt;
use tokio::sync::mpsc;

#[derive(Debug)]
enum Phase {
    Uninitialized,
    Connected,
    Authenticated,
    //    Closing
}

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

// TODO basic consume subscribe to a queue but when messages are delivered we get only the exchange
// name
pub(crate) struct ClientState {
    state: Phase,
    username: String,
    password: String,
    consumers: HashMap<Channel, MessageSink>,
    in_delivery: HashMap<Channel, DeliveredContent>,
    outgoing: mpsc::Sender<Frame>,
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
        username: "".into(),
        password: "".into(),
        consumers: HashMap::new(),
        in_delivery: HashMap::new(),
        outgoing,
    }
}

impl ClientState {
    pub(crate) async fn connection_start(&mut self, args: frame::ConnectionStartArgs) -> Result<()> {
        info!("Server supported mechanisms: {}", args.mechanisms);
        // TODO here we need to send start_ok not in the other function
        Ok(())
    }

    pub(crate) async fn connection_start_ok(&mut self, args: frame::ConnectionStartOkArgs) -> Result<()> {
        self.state = Phase::Connected;

        self.outgoing
            .send(Frame::Frame(frame::AMQPFrame::Method(
                0,
                frame::CONNECTION_START_OK,
                frame::MethodFrameArgs::ConnectionStartOk(args),
            )))
            .await?;

        Ok(())
    }

    pub(crate) async fn connection_tune(&mut self, _args: frame::ConnectionTuneArgs) -> Result<()> {
        self.state = Phase::Authenticated;

        self.outgoing.send(Frame::Frame(frame::connection_tune_ok(0))).await?;

        Ok(())
    }

    pub(crate) async fn connection_tune_ok(&mut self, _args: frame::ConnectionTuneOkArgs) -> Result<()> {
        Ok(())
    }

    pub(crate) async fn connection_open(&mut self, args: frame::ConnectionOpenArgs) -> Result<()> {
        self.outgoing
            .send(Frame::Frame(frame::connection_open(0, &args.virtual_host)))
            .await?;

        Ok(())
    }

    pub(crate) async fn connection_open_ok(&mut self) -> Result<()> {
        Ok(())
    }

    pub(crate) async fn connection_close(&mut self, _args: frame::ConnectionCloseArgs) -> Result<()> {
        self.outgoing
            .send(Frame::Frame(frame::connection_close(0, 200, "Normal close", 0, 0)))
            .await?;

        Ok(())
    }

    pub(crate) async fn connection_close_ok(&mut self) -> Result<()> {
        Ok(())
    }

    pub(crate) async fn handle_connection_close(&mut self, _args: frame::ConnectionCloseArgs) -> Result<()> {
        // TODO close resources, server is about to close connection
        Ok(())
    }

    pub(crate) async fn channel_open(&mut self, channel: Channel) -> Result<()> {
        self.outgoing.send(Frame::Frame(frame::channel_open(channel))).await?;

        Ok(())
    }

    pub(crate) async fn channel_open_ok(&mut self, _channel: Channel) -> Result<()> {
        Ok(())
    }

    pub(crate) async fn channel_close(&mut self, channel: Channel, args: frame::ChannelCloseArgs) -> Result<()> {
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

    pub(crate) async fn channel_close_ok(&mut self, channel: Channel) -> Result<()> {
        if let Some(sink) = self.consumers.remove(&channel) {
            drop(sink);
        }

        Ok(())
    }

    pub(crate) async fn handle_channel_close(
        &mut self,
        channel: Channel,
        _args: frame::ChannelCloseArgs,
    ) -> Result<()> {
        if let Some(sink) = self.consumers.remove(&channel) {
            drop(sink);
        }

        Ok(())
    }

    pub(crate) async fn exchange_declare(&mut self, channel: Channel, args: frame::ExchangeDeclareArgs) -> Result<()> {
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

    pub(crate) async fn exchange_delete(&mut self, channel: Channel, args: frame::ExchangeDeleteArgs) -> Result<()> {
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

    pub(crate) async fn queue_declare(&mut self, channel: Channel, args: frame::QueueDeclareArgs) -> Result<()> {
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

    pub(crate) async fn queue_bind(&mut self, channel: Channel, args: frame::QueueBindArgs) -> Result<()> {
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

    pub(crate) async fn queue_delete(&mut self, channel: Channel, args: frame::QueueDeleteArgs) -> Result<()> {
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

    pub(crate) async fn queue_delete_ok(&mut self, channel: Channel, args: frame::QueueDeleteOkArgs) -> Result<()> {
        Ok(())
    }

    pub(crate) async fn basic_ack(&mut self, channel: Channel, args: frame::BasicAckArgs) -> Result<()> {
        self.outgoing
            .send(Frame::Frame(frame::basic_ack(
                channel,
                args.delivery_tag,
                args.multiple,
            )))
            .await?;

        Ok(())
    }

    pub(crate) async fn basic_consume(
        &mut self,
        channel: Channel,
        args: frame::BasicConsumeArgs,
        sink: MessageSink,
    ) -> Result<()> {
        self.consumers.insert(channel, sink);

        self.outgoing
            .send(Frame::Frame(frame::basic_consume(
                channel,
                &args.queue,
                &args.consumer_tag,
                Some(args.flags),
            )))
            .await?;

        Ok(())
    }

    pub(crate) async fn basic_consume_ok(&mut self, _args: frame::BasicConsumeOkArgs) -> Result<()> {
        Ok(())
    }

    pub(crate) async fn basic_deliver(&mut self, channel: Channel, args: frame::BasicDeliverArgs) -> Result<()> {
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
        channel: Channel,
        args: frame::BasicPublishArgs,
        content: Vec<u8>,
    ) -> Result<()> {
        let fs = vec![
            frame::basic_publish(channel, &args.exchange_name, &args.routing_key),
            AMQPFrame::ContentHeader(frame::content_header(channel, content.len() as u64)),
            AMQPFrame::ContentBody(frame::content_body(channel, content.as_slice())),
        ];

        self.outgoing.send(Frame::Frames(fs)).await?;

        Ok(())
    }

    pub(crate) async fn content_header(&mut self, ch: frame::ContentHeaderFrame) -> Result<()> {
        //debug!("Content header arrived {:?}", ch);

        if let Some(dc) = self.in_delivery.get_mut(&ch.channel) {
            dc.body_size = Some(ch.body_size);
        }

        // TODO error handling

        Ok(())
    }

    pub(crate) async fn content_body(&mut self, cb: frame::ContentBodyFrame) -> Result<()> {
        //debug!("Content body arrived {:?}", cb);

        if let Some(dc) = self.in_delivery.get(&cb.channel) {
            debug!("Delivered content is {:?} so far", dc);

            debug!("Consumers {:?}", self.consumers);

            if let Some(sink) = self.consumers.get(&dc.channel) {
                let msg = Message {
                    channel: dc.channel,
                    consumer_tag: dc.consumer_tag.clone(),
                    delivery_tag: dc.delivery_tag,
                    length: dc.body_size.unwrap() as usize,
                    body: cb.body,
                };

                sink.send(msg).await?
            }
        }

        Ok(())
    }
}
