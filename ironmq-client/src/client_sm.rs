//! `client_sm` module represents the client state machine which handles incoming
//! commands (from client api side) and incoming AMQP frames from network/server
//! side.
//!
//! So everything which comes from the server or goes to the server is an
//! AMQP frame or `MethodFrame`, content etc. Everything which talks to the client
//! api it is a typed struct.

use crate::{ConsumeCallback, Result};
use ironmq_codec::frame;
use ironmq_codec::frame::{Channel};
//use log::{error, info};
use std::collections::HashMap;
use std::fmt;

#[derive(Debug)]
enum Phase {
    Uninitialized,
    Connected,
    Authenticated,
    //    Closing
}

pub(crate) struct ClientState {
    state: Phase,
    username: String,
    password: String,
    consumers: HashMap<(u16, String), ConsumeCallback>,
    headers: HashMap<u16, DeliveredContent>,
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

pub(crate) trait Client {
    fn connection_start(&mut self, args: frame::ConnectionStartArgs) -> MaybeFrame;
    fn connection_start_ok(&mut self, args: frame::ConnectionStartOkArgs) -> MaybeFrame;
    fn connection_tune(&mut self, args: frame::ConnectionTuneArgs) -> MaybeFrame;
    fn connection_tune_ok(&mut self, args: frame::ConnectionTuneOkArgs) -> MaybeFrame;
    fn connection_open(&mut self, args: frame::ConnectionOpenArgs) -> MaybeFrame;
    fn connection_open_ok(&mut self) -> MaybeFrame;
    fn connection_close(&mut self, args: frame::ConnectionCloseArgs) -> MaybeFrame;
    fn connection_close_ok(&mut self) -> MaybeFrame;

    fn channel_open(&mut self, channel: Channel) -> MaybeFrame;
    fn channel_open_ok(&mut self, channel: Channel) -> MaybeFrame;
    fn channel_close(&mut self) -> MaybeFrame;

    fn exchange_declare(&mut self, channel: Channel, args: frame::ExchangeDeclareArgs) -> MaybeFrame;
    fn exchange_declare_ok(&mut self) -> MaybeFrame;

    fn queue_declare(&mut self, channel: Channel, args: frame::QueueDeclareArgs) -> MaybeFrame;
    fn queue_declare_ok(&mut self, args: frame::QueueDeclareOkArgs) -> MaybeFrame;
    fn queue_bind(&mut self, channel: Channel, args: frame::QueueBindArgs) -> MaybeFrame;
    fn queue_bind_ok(&mut self) -> MaybeFrame;

    fn basic_consume(&mut self, channel: Channel, args: frame::BasicConsumeArgs) -> MaybeFrame;
    fn basic_consume_ok(&mut self, args: frame::BasicConsumeOkArgs) -> MaybeFrame;
    fn basic_deliver(&mut self, args: frame::BasicDeliverArgs) -> MaybeFrame;
    fn basic_publish(&mut self, channel: Channel, args: frame::BasicPublishArgs) -> MaybeFrame;

    fn content_header(&mut self, ch: frame::ContentHeaderFrame) -> MaybeFrame;
    fn content_body(&mut self, ch: frame::ContentBodyFrame) -> MaybeFrame;
}

pub(crate) fn new() -> ClientState {
    ClientState {
        state: Phase::Uninitialized,
        username: "guest".into(),
        password: "guest".into(),
        consumers: HashMap::<(u16, String), ConsumeCallback>::new(),
        headers: HashMap::<u16, DeliveredContent>::new(),
    }
}

impl Client for ClientState {
    fn connection_start(&mut self, args: frame::ConnectionStartArgs) -> MaybeFrame {
        // TODO analyse server capabilities
        Ok(None)
    }

    fn connection_start_ok(&mut self, args: frame::ConnectionStartOkArgs) -> MaybeFrame {
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

    fn connection_tune(&mut self, args: frame::ConnectionTuneArgs) -> MaybeFrame {
        self.state = Phase::Authenticated;

        Ok(Some(frame::connection_tune_ok(0)))
    }

    fn connection_tune_ok(&mut self, args: frame::ConnectionTuneOkArgs) -> MaybeFrame {
        Ok(None)
    }

    fn connection_open(&mut self, args: frame::ConnectionOpenArgs) -> MaybeFrame {
        Ok(Some(frame::connection_open(0, args.virtual_host)))
    }

    fn connection_open_ok(&mut self) -> MaybeFrame {
        Ok(None)
    }

    fn connection_close(&mut self, args: frame::ConnectionCloseArgs) -> MaybeFrame {
        Ok(Some(frame::connection_close(0)))
    }

    fn connection_close_ok(&mut self) -> MaybeFrame {
        Ok(None)
    }

    fn channel_open(&mut self, channel: frame::Channel) -> MaybeFrame {
        Ok(Some(frame::channel_open(channel)))
    }

    fn channel_open_ok(&mut self, channel: frame::Channel) -> MaybeFrame {
        Ok(None)
    }

    fn channel_close(&mut self) -> MaybeFrame {
        Ok(None)
    }

    fn exchange_declare(&mut self, channel: Channel, args: frame::ExchangeDeclareArgs) -> MaybeFrame {
        Ok(Some(frame::exchange_declare(channel, args.exchange_name, args.exchange_type)))
    }

    fn exchange_declare_ok(&mut self) -> MaybeFrame {
        Ok(None)
    }

    fn queue_declare(&mut self, channel: Channel, args: frame::QueueDeclareArgs) -> MaybeFrame {
        Ok(Some(frame::queue_declare(channel, args.name)))
    }

    fn queue_declare_ok(&mut self, args: frame::QueueDeclareOkArgs) -> MaybeFrame {
        Ok(None)
    }

    fn queue_bind(&mut self, channel: Channel, args: frame::QueueBindArgs) -> MaybeFrame {
        Ok(Some(frame::queue_bind(channel, args.queue_name, args.exchange_name, args.routing_key)))
    }

    fn queue_bind_ok(&mut self) -> MaybeFrame {
        Ok(None)
    }

    fn basic_consume(&mut self, channel: Channel, args: frame::BasicConsumeArgs) -> MaybeFrame {
        Ok(Some(frame::basic_consume(channel, args.queue, args.consumer_tag)))
    }

    fn basic_consume_ok(&mut self, args: frame::BasicConsumeOkArgs) -> MaybeFrame {
        Ok(None)
    }

    fn basic_deliver(&mut self, args: frame::BasicDeliverArgs) -> MaybeFrame {
        Ok(None)
    }

    fn basic_publish(&mut self, channel: Channel, args: frame::BasicPublishArgs) -> MaybeFrame {
        Ok(Some(frame::basic_publish(channel, args.exchange_name, args.routing_key)))
    }

    fn content_header(&mut self, ch: frame::ContentHeaderFrame) -> MaybeFrame {
        Ok(None)
    }

    fn content_body(&mut self, ch: frame::ContentBodyFrame) -> MaybeFrame {
        Ok(None)
    }
}

struct DeliveredContent {
    channel: u16,
    consumer_tag: String,
    header: Option<frame::ContentHeaderFrame>, // flags
}

//pub(crate) fn basic_consume(
//    cs: &mut ClientState,
//    args: BasicConsumeArgs,
//) -> Result<Option<MethodFrame>> {
//    // TODO we shouldn't finalize the subscription here, because we
//    cs.consumers
//        .insert((args.channel, args.consumer_tag.clone()), args.callback);
//
//    Ok(Some(frame::basic_consume(
//        args.channel,
//        args.queue_name,
//        args.consumer_tag,
//    )))
//}
//
//pub(crate) fn basic_consume_ok(
//    _cs: &mut ClientState,
//    _f: MethodFrame,
//) -> Result<Option<MethodFrame>> {
//    Ok(None)
//}
//
//pub(crate) fn basic_deliver(
//    cs: &mut ClientState,
//    mut f: MethodFrame,
//) -> Result<Option<MethodFrame>> {
//    // TODO validate consumer tag and channel, probably we need to send an error if we get a
//    // content but we are not consuming
//    let consumer_tag = frame::value_as_string(f.args.remove(0))?;
//
//    let content = DeliveredContent {
//        channel: f.channel,
//        consumer_tag: consumer_tag,
//        header: None,
//    };
//
//    cs.headers.insert(f.channel, content);
//
//    Ok(None)
//}
//
//pub(crate) fn basic_publish(
//    _cs: &mut ClientState,
//    args: BasicPublishArgs,
//) -> Result<Option<MethodFrame>> {
//    Ok(Some(frame::basic_publish(
//        args.channel,
//        args.exchange_name,
//        args.routing_key,
//    )))
//}
//
//pub(crate) fn content_header(
//    cs: &mut ClientState,
//    ch: ContentHeaderFrame,
//) -> Result<Option<AMQPFrame>> {
//    info!("Received content header {:?}", ch);
//
//    if let Some(h) = cs.headers.get_mut(&ch.channel) {
//        h.header = Some(ch)
//    }
//
//    Ok(None)
//}
//
//pub(crate) fn content_body(
//    cs: &mut ClientState,
//    cb: ContentBodyFrame,
//) -> Result<Option<AMQPFrame>> {
//    info!("Received body {:?}", cb);
//
//    if let Some(h) = cs.headers.remove(&cb.channel) {
//        let body = String::from_utf8(cb.body)?;
//
//        if let Some(handler) = cs.consumers.get(&(cb.channel, h.consumer_tag)) {
//            handler(body);
//        }
//    }
//
//    Ok(None)
//}
//

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn connect() {}
}
