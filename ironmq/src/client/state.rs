use crate::{Context, Result, RuntimeError};
use crate::exchange;
use crate::message;
use ironmq_codec::frame;
use ironmq_codec::frame::{AMQPFrame, Channel};
use log::info;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

pub(crate) type MaybeFrame = Result<Option<AMQPFrame>>;

pub(crate) const NOT_FOUND: u16 = 404;
pub(crate) const PRECONDITION_FAILED: u16 = 406;
pub(crate) const CHANNEL_ERROR: u16 = 504;
pub(crate) const NOT_ALLOWED: u16 = 530;

/// All the transient data of a connection are stored here.
pub(crate) struct Connection {
    context: Arc<Mutex<Context>>,
    virtual_host: String,
    open_channels: HashMap<Channel, ()>,
    exchanges: HashMap<String, mpsc::Sender<message::Message>>,
    queues: HashMap<String, ()>,
    /// Simple exchange-queue binding
    binding: HashMap<(String, String), ()>,
    in_flight_contents: HashMap<Channel, PublishedContent>
}

#[derive(Debug)]
struct PublishedContent {
    channel: Channel,
    exchange: String,
    length: Option<u64>,
    content: Option<Vec<u8>>
}

pub(crate) fn new(context: Arc<Mutex<Context>>) -> Connection {
    Connection {
        context: context,
        virtual_host: "".into(),
        open_channels: HashMap::new(),
        exchanges: HashMap::new(),
        queues: HashMap::new(),
        binding: HashMap::new(),
        in_flight_contents: HashMap::new()
    }
}

pub(crate) async fn connection_open(conn: &mut Connection, channel: Channel,
                                    args: frame::ConnectionOpenArgs) -> MaybeFrame {
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
        channel_error(channel, CHANNEL_ERROR, "Channel already opened", frame::CHANNEL_OPEN)
    } else {
        conn.open_channels.insert(channel, ());
        Ok(Some(frame::channel_open_ok(channel)))
    }
}

pub(crate) async fn channel_close(conn: &mut Connection, channel: Channel,
                                  args: frame::ChannelCloseArgs) -> MaybeFrame {
    conn.open_channels.remove(&channel);
    Ok(Some(frame::channel_close_ok(channel)))
}

pub(crate) async fn exchange_declare(conn: &mut Connection, channel: Channel,
                                     args: frame::ExchangeDeclareArgs) -> MaybeFrame {
    let no_wait = args.flags.contains(frame::ExchangeDeclareFlags::NO_WAIT);
    let mut ctx = conn.context.lock().await;
    let result = exchange::declare(&mut ctx.exchanges, &args).await;

    match result {
        Ok(ch) => {
            conn.exchanges.insert(args.exchange_name.clone(), ch);

            if no_wait {
                Ok(None)
            } else {
                Ok(Some(frame::exchange_declare_ok(channel)))
            }
        }
        Err(e) => match e.downcast::<RuntimeError>() {
            Ok(rte) => Ok(Some(AMQPFrame::from(*rte))),
            Err(e2) => Err(e2),
        },
    }
}

pub(crate) async fn queue_declare(conn: &mut Connection, channel: Channel,
                                  args: frame::QueueDeclareArgs,) -> MaybeFrame {
    if !conn.queues.contains_key(&args.name) {
        conn.queues.insert(args.name.clone(), ());
    }

    Ok(Some(frame::queue_declare_ok(channel, args.name, 0, 0)))
}

pub(crate) async fn queue_bind(conn: &mut Connection, channel: Channel,
                               args: frame::QueueBindArgs,) -> MaybeFrame {
    let binding = (args.exchange_name, args.queue_name);

    if !conn.binding.contains_key(&binding) {
        conn.binding.insert(binding, ());
    }

    Ok(Some(frame::queue_bind_ok(channel)))
}

pub(crate) async fn basic_publish(conn: &mut Connection, channel: Channel,
                                  args: frame::BasicPublishArgs) -> MaybeFrame {

    if !conn.exchanges.contains_key(&args.exchange_name) {
        channel_error(channel, NOT_FOUND, "Exchange not found", frame::BASIC_PUBLISH)
    } else {
        // TODO check if there is in flight content in the channel -> error
        conn.in_flight_contents.insert(channel, PublishedContent {
            channel: channel,
            exchange: args.exchange_name,
            length: None,
            content: None
        });

        Ok(None)
    }
}

pub(crate) async fn receive_content_header(conn: &mut Connection, header: frame::ContentHeaderFrame) -> MaybeFrame {
    // TODO collect info into a data struct
    info!("Receive content with length {}", header.body_size);

    if let Some(pc) = conn.in_flight_contents.get_mut(&header.channel) {
        pc.length = Some(header.body_size);
    }

    Ok(None)
}

pub(crate) async fn receive_content_body(conn: &mut Connection, body: frame::ContentBodyFrame) -> MaybeFrame {
    info!("Receive content with length {}", body.body.len());

    if let Some(pc) = conn.in_flight_contents.remove(&body.channel) {
        let msg = message::Message {
            content: body.body,
            processed: None
        };

        match conn.exchanges.get(&pc.exchange) {
            Some(ch) => {
                ch.send(msg).await;
                Ok(None)
            },
            None =>
                // TODO error, exchange cannot be found
                Ok(None)
        }
    } else {
        Ok(None)
    }
}

fn channel_error(channel: Channel, code: u16, text: &str, cm_id: u32) -> MaybeFrame {
    let (cid, mid) = frame::split_class_method(cm_id);

    Ok(Some(frame::channel_close(
                channel,
                code,
                text,
                cid,
                mid)))
}
