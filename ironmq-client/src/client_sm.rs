//! `client_sm` module represents the client state machine which handles incoming
//! commands (from client api side) and incoming AMQP frames from network/server
//! side.
//!
//! So everything which comes from the server or goes to the server is an
//! AMQP frame or `MethodFrame`, content etc. Everything which talks to the client
//! api it is a typed struct.

use crate::{ConsumeCallback, Result};
use ironmq_codec::frame;
use ironmq_codec::frame::{
    AMQPFieldValue, AMQPFrame, ContentBodyFrame, ContentHeaderFrame, MethodFrameArgs,
};
use log::{error, info};
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

type MaybeFrame = Result<Option<AMQPFrame>>;

pub(crate) trait Client {
    fn connection_start(&mut self, args: frame::MethodFrameArgs) -> MaybeFrame;
    fn connection_start_ok(&mut self, args: frame::MethodFrameArgs) -> MaybeFrame;
    fn connection_tune(&mut self, args: frame::MethodFrameArgs) -> MaybeFrame;
    fn connection_tune_ok(&mut self, args: frame::MethodFrameArgs) -> MaybeFrame;
    fn connection_open(&mut self, args: frame::ConnectionOpenArgs) -> MaybeFrame;
    fn connection_open_ok(&mut self, args: frame::MethodFrameArgs) -> MaybeFrame;
    fn connection_close(&mut self, args: frame::MethodFrameArgs) -> MaybeFrame;
    fn connection_close_ok(&mut self, args: frame::MethodFrameArgs) -> MaybeFrame;

    fn channel_open(&mut self, args: frame::MethodFrameArgs) -> MaybeFrame;
    fn channel_open_ok(&mut self, args: frame::MethodFrameArgs) -> MaybeFrame;
    fn channel_close(&mut self, args: frame::MethodFrameArgs) -> MaybeFrame;

    fn exchange_declare(&mut self, args: frame::MethodFrameArgs) -> MaybeFrame;
    fn exchange_declare_ok(&mut self, args: frame::MethodFrameArgs) -> MaybeFrame;

    fn queue_declare(&mut self, args: frame::MethodFrameArgs) -> MaybeFrame;
    fn queue_declare_ok(&mut self, args: frame::MethodFrameArgs) -> MaybeFrame;
    fn queue_bind(&mut self, args: frame::MethodFrameArgs) -> MaybeFrame;
    fn queue_bind_ok(&mut self, args: frame::MethodFrameArgs) -> MaybeFrame;

    fn basic_consume(&mut self, args: frame::MethodFrameArgs) -> MaybeFrame;
    fn basic_consume_ok(&mut self, args: frame::MethodFrameArgs) -> MaybeFrame;
    fn basic_deliver(&mut self, args: frame::MethodFrameArgs) -> MaybeFrame;
    fn basic_publish(&mut self, args: frame::MethodFrameArgs) -> MaybeFrame;
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
    fn connection_start(&mut self, args: frame::MethodFrameArgs) -> MaybeFrame {
        // TODO analyse server capabilities
        Ok(None)
    }

    fn connection_start_ok(&mut self, args: frame::MethodFrameArgs) -> MaybeFrame {
        let mut caps = HashMap::<String, AMQPFieldValue>::new();

        caps.insert(
            "authentication_failure_on_close".into(),
            AMQPFieldValue::Bool(true),
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

    fn connection_tune(&mut self, args: frame::MethodFrameArgs) -> MaybeFrame {
        self.state = Phase::Authenticated;

        Ok(Some(frame::connection_tune_ok(0)))
    }

    fn connection_tune_ok(&mut self, args: frame::MethodFrameArgs) -> MaybeFrame {
        Ok(None)
    }

    fn connection_open(&mut self, args: frame::ConnectionOpenArgs) -> Result<Option<AMQPFrame>> {
        Ok(Some(frame::connection_open(0, args.virtual_host)))
    }

    fn connection_open_ok(&mut self, args: frame::MethodFrameArgs) -> MaybeFrame {
        Ok(None)
    }

    fn connection_close(&mut self, args: frame::MethodFrameArgs) -> MaybeFrame {
        Ok(Some(frame::connection_close(0)))
    }

    fn connection_close_ok(&mut self, args: frame::MethodFrameArgs) -> MaybeFrame {
        Ok(None)
    }

    fn channel_open(&mut self, args: frame::MethodFrameArgs) -> MaybeFrame {
        Ok(Some(frame::channel_open(args.channel)))
    }

    fn channel_open_ok(&mut self, args: frame::MethodFrameArgs) -> MaybeFrame {
        Ok(None)
    }

    fn channel_close(&mut self, args: frame::MethodFrameArgs) -> MaybeFrame {
        Ok(None)
    }

    fn exchange_declare(&mut self, args: frame::MethodFrameArgs) -> MaybeFrame;
    fn exchange_declare_ok(&mut self, args: frame::MethodFrameArgs) -> MaybeFrame;

    fn queue_declare(&mut self, args: frame::MethodFrameArgs) -> MaybeFrame;
    fn queue_declare_ok(&mut self, args: frame::MethodFrameArgs) -> MaybeFrame;
    fn queue_bind(&mut self, args: frame::MethodFrameArgs) -> MaybeFrame;
    fn queue_bind_ok(&mut self, args: frame::MethodFrameArgs) -> MaybeFrame;

    fn basic_consume(&mut self, args: frame::MethodFrameArgs) -> MaybeFrame;
    fn basic_consume_ok(&mut self, args: frame::MethodFrameArgs) -> MaybeFrame;
    fn basic_deliver(&mut self, args: frame::MethodFrameArgs) -> MaybeFrame;
    fn basic_publish(&mut self, args: frame::MethodFrameArgs) -> MaybeFrame;
}

struct DeliveredContent {
    channel: u16,
    consumer_tag: String,
    header: Option<ContentHeaderFrame>, // flags
}

//pub(crate) fn exchange_declare(
//    _cs: &mut ClientState,
//    args: ExchangeDeclareArgs,
//) -> Result<Option<MethodFrame>> {
//    Ok(Some(frame::exchange_declare(
//        args.channel,
//        args.exchange_name,
//        args.exchange_type,
//    )))
//}
//
//pub(crate) fn exchange_declare_ok(
//    _cs: &mut ClientState,
//    _f: MethodFrame,
//) -> Result<Option<MethodFrame>> {
//    Ok(None)
//}
//
//pub(crate) fn queue_declare(
//    _cs: &mut ClientState,
//    args: QueueDeclareArgs,
//) -> Result<Option<MethodFrame>> {
//    Ok(Some(frame::queue_declare(args.channel, args.queue_name)))
//}
//
//pub(crate) fn queue_declare_ok(
//    _cs: &mut ClientState,
//    _f: MethodFrame,
//) -> Result<Option<MethodFrame>> {
//    Ok(None)
//}
//
//pub(crate) fn queue_bind(
//    _cs: &mut ClientState,
//    args: QueueBindArgs,
//) -> Result<Option<MethodFrame>> {
//    Ok(Some(frame::queue_bind(
//        args.channel,
//        args.queue_name,
//        args.exchange_name,
//        args.routing_key,
//    )))
//}
//
//pub(crate) fn queue_bind_ok(_cs: &mut ClientState, _f: MethodFrame) -> Result<Option<MethodFrame>> {
//    Ok(None)
//}
//
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
#[derive(Debug)]
pub(crate) struct ConnOpenArgs {
    pub(crate) virtual_host: String,
    pub(crate) insist: bool,
}

#[derive(Debug)]
pub(crate) struct ConnCloseArgs {}

#[derive(Debug)]
pub(crate) struct ChannelOpenArgs {
    pub(crate) channel: u16,
}

#[derive(Debug)]
pub(crate) struct ExchangeDeclareArgs {
    pub(crate) channel: u16,
    pub(crate) exchange_name: String,
    pub(crate) exchange_type: String,
}

#[derive(Debug)]
pub(crate) struct QueueDeclareArgs {
    pub(crate) channel: u16,
    pub(crate) queue_name: String,
}

#[derive(Debug)]
pub(crate) struct QueueBindArgs {
    pub(crate) channel: u16,
    pub(crate) queue_name: String,
    pub(crate) exchange_name: String,
    pub(crate) routing_key: String,
}

pub(crate) struct BasicConsumeArgs {
    pub(crate) channel: u16,
    pub(crate) queue_name: String,
    pub(crate) consumer_tag: String,
    pub(crate) no_local: bool,
    pub(crate) no_ack: bool,
    pub(crate) exclusive: bool,
    pub(crate) no_wait: bool,
    pub(crate) arguments: HashMap<String, AMQPFieldValue>,
    pub(crate) callback: ConsumeCallback,
}

impl fmt::Debug for BasicConsumeArgs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BasicConsumeArgs")
            .field("channel", &self.channel)
            .field("queue_name", &self.queue_name)
            .field("consumer_tag", &self.consumer_tag)
            .field("no_local", &self.no_local)
            .field("no_ack", &self.no_ack)
            .field("exclusive", &self.exclusive)
            .field("no_wait", &self.no_wait)
            .field("arguments", &self.arguments)
            .finish()
    }
}

#[derive(Debug)]
pub(crate) struct BasicPublishArgs {
    pub(crate) channel: u16,
    pub(crate) exchange_name: String,
    pub(crate) routing_key: String,
    pub(crate) mandatory: bool, // immediate is always true
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn connect() {}
}
