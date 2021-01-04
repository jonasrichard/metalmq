//! `client_sm` module represents the client state machine which handles incoming
//! commands (from client api side) and incoming AMQP frames from network/server
//! side.
//!
//! So everything which comes from the server or goes to the server is an
//! AMQP frame or `MethodFrame`, content etc. Everything which talks to the client
//! api it is a typed struct.

use crate::{Message, MessageSink, Result};
use ironmq_codec::frame::{self, Channel};
use log::{debug, info};
use std::collections::HashMap;
use std::fmt;

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
    body: Option<Vec<u8>>
}

// TODO basic consume subscribe to a queue but when messages are delivered we get only the exchange
// name
pub(crate) struct ClientState {
    state: Phase,
    username: String,
    password: String,
    consumers: HashMap<Channel, MessageSink>,
    in_delivery: HashMap<Channel, DeliveredContent>,
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

type MaybeFrame = Result<Option<frame::AMQPFrame>>;

#[async_trait]
pub(crate) trait Client {
    async fn connection_start(&mut self, args: frame::ConnectionStartArgs) -> MaybeFrame;
    async fn connection_start_ok(&mut self, args: frame::ConnectionStartOkArgs) -> MaybeFrame;
    async fn connection_tune(&mut self, args: frame::ConnectionTuneArgs) -> MaybeFrame;
    async fn connection_tune_ok(&mut self, args: frame::ConnectionTuneOkArgs) -> MaybeFrame;
    async fn connection_open(&mut self, args: frame::ConnectionOpenArgs) -> MaybeFrame;
    async fn connection_open_ok(&mut self) -> MaybeFrame;
    async fn connection_close(&mut self, args: frame::ConnectionCloseArgs) -> MaybeFrame;
    async fn connection_close_ok(&mut self) -> MaybeFrame;
    async fn handle_connection_close(&mut self, args: frame::ConnectionCloseArgs) -> MaybeFrame;

    async fn channel_open(&mut self, channel: Channel) -> MaybeFrame;
    async fn channel_open_ok(&mut self, channel: Channel) -> MaybeFrame;
    async fn channel_close(&mut self, channel: Channel, args: frame::ChannelCloseArgs) -> MaybeFrame;
    async fn channel_close_ok(&mut self, channel: Channel) -> MaybeFrame;
    async fn handle_channel_close(&mut self, channel: Channel, args: frame::ChannelCloseArgs) -> MaybeFrame;

    async fn exchange_declare(&mut self, channel: Channel, args: frame::ExchangeDeclareArgs) -> MaybeFrame;
    async fn exchange_declare_ok(&mut self) -> MaybeFrame;
    async fn exchange_bind(&mut self, channel: Channel, args: frame::ExchangeBindArgs) -> MaybeFrame;
    async fn exchange_bind_ok(&mut self) -> MaybeFrame;

    async fn queue_declare(&mut self, channel: Channel, args: frame::QueueDeclareArgs) -> MaybeFrame;
    async fn queue_declare_ok(&mut self, args: frame::QueueDeclareOkArgs) -> MaybeFrame;
    async fn queue_bind(&mut self, channel: Channel, args: frame::QueueBindArgs) -> MaybeFrame;
    async fn queue_bind_ok(&mut self) -> MaybeFrame;

    async fn basic_consume(&mut self, channel: Channel, args: frame::BasicConsumeArgs, sink: MessageSink) -> MaybeFrame;
    async fn basic_consume_ok(&mut self, args: frame::BasicConsumeOkArgs) -> MaybeFrame;
    async fn basic_deliver(&mut self, channel: Channel, args: frame::BasicDeliverArgs) -> MaybeFrame;
    async fn basic_publish(&mut self, channel: Channel, args: frame::BasicPublishArgs) -> MaybeFrame;

    async fn content_header(&mut self, ch: frame::ContentHeaderFrame) -> MaybeFrame;
    async fn content_body(&mut self, ch: frame::ContentBodyFrame) -> MaybeFrame;
}

pub(crate) fn new() -> ClientState {
    ClientState {
        state: Phase::Uninitialized,
        username: "guest".into(),
        password: "guest".into(),
        consumers: HashMap::new(),
        in_delivery: HashMap::new(),
    }
}

#[async_trait]
impl Client for ClientState {
    async fn connection_start(&mut self, args: frame::ConnectionStartArgs) -> MaybeFrame {
        info!("Server supported mechanisms: {}", args.mechanisms);
        // TODO here we need to send start_ok not in the other function
        Ok(None)
    }

    async fn connection_start_ok(&mut self, _args: frame::ConnectionStartOkArgs) -> MaybeFrame {
        self.state = Phase::Connected;

        let mut caps = frame::FieldTable::new();

        caps.insert(
            "authentication_failure_on_close".into(),
            frame::AMQPFieldValue::Bool(true),
        );

        //capabilities.insert("basic.nack".into(), AMQPFieldValue::Bool(true));
        //capabilities.insert("connection.blocked".into(), AMQPFieldValue::Bool(true));
        //capabilities.insert("consumer_cancel_notify".into(), AMQPFieldValue::Bool(true));
        //capabilities.insert("publisher_confirms".into(), AMQPFieldValue::Bool(true));

        Ok(Some(frame::connection_start_ok(
            &self.username,
            &self.password,
            caps,
        )))
    }

    async fn connection_tune(&mut self, _args: frame::ConnectionTuneArgs) -> MaybeFrame {
        self.state = Phase::Authenticated;

        Ok(Some(frame::connection_tune_ok(0)))
    }

    async fn connection_tune_ok(&mut self, _args: frame::ConnectionTuneOkArgs) -> MaybeFrame {
        Ok(None)
    }

    async fn connection_open(&mut self, args: frame::ConnectionOpenArgs) -> MaybeFrame {
        Ok(Some(frame::connection_open(0, args.virtual_host)))
    }

    async fn connection_open_ok(&mut self) -> MaybeFrame {
        Ok(None)
    }

    async fn connection_close(&mut self, _args: frame::ConnectionCloseArgs) -> MaybeFrame {
        Ok(Some(frame::connection_close(0, 200, "Normal close", 0, 0)))
    }

    async fn connection_close_ok(&mut self) -> MaybeFrame {
        Ok(None)
    }

    async fn handle_connection_close(&mut self, args: frame::ConnectionCloseArgs) -> MaybeFrame {
        // TODO close resources, server is about to close connection
        Ok(None)
    }

    async fn channel_open(&mut self, channel: Channel) -> MaybeFrame {
        Ok(Some(frame::channel_open(channel)))
    }

    async fn channel_open_ok(&mut self, _channel: Channel) -> MaybeFrame {
        Ok(None)
    }

    async fn channel_close(&mut self, channel: Channel, args: frame::ChannelCloseArgs) -> MaybeFrame {
        Ok(Some(frame::channel_close(channel, args.code, &args.text, args.class_id, args.method_id)))
    }

    async fn channel_close_ok(&mut self, channel: Channel) -> MaybeFrame {
        if let Some(sink) = self.consumers.remove(&channel) {
            drop(sink);
        }

        Ok(None)
    }

    async fn handle_channel_close(&mut self, channel: Channel, args: frame::ChannelCloseArgs) -> MaybeFrame {
        // TODO handle that the server closed the channel
        //Ok(Some(frame::channel_close_ok(channel)))
        Ok(None)
    }

    async fn exchange_declare(&mut self, channel: Channel, args: frame::ExchangeDeclareArgs) -> MaybeFrame {
        Ok(Some(frame::exchange_declare(channel, args.exchange_name, args.exchange_type, Some(args.flags))))
    }

    async fn exchange_declare_ok(&mut self) -> MaybeFrame {
        Ok(None)
    }

    async fn exchange_bind(&mut self, _channel: Channel, _args: frame::ExchangeBindArgs) -> MaybeFrame {
        unimplemented!()
    }

    async fn exchange_bind_ok(&mut self) -> MaybeFrame {
        Ok(None)
    }

    async fn queue_declare(&mut self, channel: Channel, args: frame::QueueDeclareArgs) -> MaybeFrame {
        Ok(Some(frame::queue_declare(channel, args.name)))
    }

    async fn queue_declare_ok(&mut self, _args: frame::QueueDeclareOkArgs) -> MaybeFrame {
        Ok(None)
    }

    async fn queue_bind(&mut self, channel: Channel, args: frame::QueueBindArgs) -> MaybeFrame {
        Ok(Some(frame::queue_bind(channel, args.queue_name, args.exchange_name, args.routing_key)))
    }

    async fn queue_bind_ok(&mut self) -> MaybeFrame {
        Ok(None)
    }

    async fn basic_consume(&mut self, channel: Channel, args: frame::BasicConsumeArgs, sink: MessageSink) -> MaybeFrame {
        self.consumers.insert(channel, sink);

        Ok(Some(frame::basic_consume(channel, args.queue, args.consumer_tag)))
    }

    async fn basic_consume_ok(&mut self, _args: frame::BasicConsumeOkArgs) -> MaybeFrame {
        Ok(None)
    }

    async fn basic_deliver(&mut self, channel: Channel, args: frame::BasicDeliverArgs) -> MaybeFrame {
        let dc = DeliveredContent {
            channel: channel,
            consumer_tag: args.consumer_tag,
            delivery_tag: args.delivery_tag,
            exchange_name: args.exchange_name,
            routing_key: args.routing_key,
            body_size: None,
            body: None
        };

        self.in_delivery.insert(channel, dc);

        Ok(None)
    }

    async fn basic_publish(&mut self, channel: Channel, args: frame::BasicPublishArgs) -> MaybeFrame {
        Ok(Some(frame::basic_publish(channel, args.exchange_name, args.routing_key)))
    }

    async fn content_header(&mut self, ch: frame::ContentHeaderFrame) -> MaybeFrame {
        info!("Content header arrived {:?}", ch);

        if let Some(dc) = self.in_delivery.get_mut(&ch.channel) {
            dc.body_size = Some(ch.body_size);
        }

        // TODO error handling

        Ok(None)
    }

    async fn content_body(&mut self, cb: frame::ContentBodyFrame) -> MaybeFrame {
        info!("Content body arrived {:?}", cb);

        if let Some(dc) = self.in_delivery.get(&cb.channel) {
            debug!("Delivered content is {:?} so far", dc);

            debug!("Consumers {:?}", self.consumers);

            if let Some(sink) = self.consumers.get(&dc.channel) {
                let msg = Message {
                    channel: dc.channel,
                    body: cb.body,
                    length: dc.body_size.unwrap() as usize
                };

                sink.send(msg).await?
            }
        }

        Ok(None)
    }
}
