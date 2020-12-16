use crate::{Context, Result};
use ironmq_codec::frame;
use ironmq_codec::frame::{AMQPFrame, Channel};
use log::info;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub(crate) type MaybeFrame = Result<Option<AMQPFrame>>;

pub(crate) const NOT_FOUND: u16 = 404;
pub(crate) const PRECONDITION_FAILED: u16 = 406;
pub(crate) const CHANNEL_ERROR: u16 = 504;
pub(crate) const NOT_ALLOWED: u16 = 530;

pub(crate) struct Connection {
    context: Arc<Mutex<Context>>,
    virtual_host: String,
    open_channels: HashMap<Channel, ()>,
    exchanges: HashMap<String, ()>,
    queues: HashMap<String, ()>,
    /// Simple exchange-queue binding
    binding: HashMap<(String, String), ()>
}

pub(crate) fn new(context: Arc<Mutex<Context>>) -> Connection {
    Connection {
        context: context,
        virtual_host: "".into(),
        open_channels: HashMap::new(),
        exchanges: HashMap::new(),
        queues: HashMap::new(),
        binding: HashMap::new()
    }
}

pub(crate) async fn connection_open(conn: &mut Connection, channel: Channel, args: frame::ConnectionOpenArgs) -> MaybeFrame {
    // TODO check if virtual host doens't exist
    conn.virtual_host = args.virtual_host;
    Ok(Some(frame::connection_open_ok(channel)))
}

pub(crate) async fn connection_close(conn: &mut Connection, args: frame::ConnectionCloseArgs) -> MaybeFrame {
    // TODO cleanup
    Ok(Some(frame::connection_close_ok(0)))
}

pub(crate) async fn channel_open(conn: &mut Connection, channel: Channel) -> MaybeFrame {
    if conn.open_channels.contains_key(&channel) {
        let (cid, mid) = frame::split_class_method(frame::CHANNEL_OPEN);
        Ok(Some(frame::channel_close(channel, CHANNEL_ERROR, "Channel already opened", cid, mid)))
    } else {
        conn.open_channels.insert(channel, ());
        Ok(Some(frame::channel_open_ok(channel)))
    }
}

pub(crate) async fn channel_close(conn: &mut Connection, channel: Channel, args: frame::ChannelCloseArgs) -> MaybeFrame {
    conn.open_channels.remove(&channel);
    Ok(Some(frame::channel_close_ok(channel)))
}

pub(crate) async fn exchange_declare(conn: &mut Connection, channel: Channel, args: frame::ExchangeDeclareArgs) -> MaybeFrame {
    if conn.exchanges.contains_key(&args.exchange_name) {
        let (cid, mid) = frame::split_class_method(frame::EXCHANGE_DECLARE);
        Ok(Some(frame::channel_close(channel, NOT_ALLOWED, "Exchange already exists", cid, mid)))
    } else {
        conn.exchanges.insert(args.exchange_name, ());
        Ok(Some(frame::exchange_declare_ok(channel)))
    }
}

pub(crate) async fn queue_declare(conn: &mut Connection, channel: Channel, args: frame::QueueDeclareArgs) -> MaybeFrame {
    if !conn.queues.contains_key(&args.name) {
        conn.queues.insert(args.name.clone(), ());
    }

    Ok(Some(frame::queue_declare_ok(channel, args.name, 0, 0)))
}

pub(crate) async fn queue_bind(conn: &mut Connection, channel: Channel, args: frame::QueueBindArgs) -> MaybeFrame {
    let binding = (args.exchange_name, args.queue_name);

    if !conn.binding.contains_key(&binding) {
        conn.binding.insert(binding, ());
    }

    Ok(Some(frame::queue_bind_ok(channel)))
}

pub(crate) async fn basic_publish(conn: &mut Connection, channel: Channel, args: frame::BasicPublishArgs) -> MaybeFrame {
    if !conn.exchanges.contains_key(&args.exchange_name) {
        let (cid, mid) = frame::split_class_method(frame::BASIC_PUBLISH);
        Ok(Some(frame::channel_close(channel, NOT_FOUND, "Exchange not found", cid, mid)))
    } else {
        Ok(None)
    }
}

pub(crate) async fn receive_content_header(conn: &mut Connection, header: frame::ContentHeaderFrame) -> MaybeFrame {
    // TODO collect info into a data struct
    info!("Receive content with length {}", header.body_size);
    Ok(None)
}

pub(crate) async fn receive_content_body(conn: &mut Connection, body: frame::ContentBodyFrame) -> MaybeFrame {
    info!("Receive content with length {}", body.body.len());
    Ok(None)
}
