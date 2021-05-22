use crate::exchange::{handler::ExchangeCommand, handler::ExchangeCommandSink};
use crate::message;
use crate::{Context, Result, RuntimeError};
use log::info;
use metalmq_codec::frame::{self, AMQPFrame, Channel};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use uuid::Uuid;

pub(crate) type MaybeFrame = Result<Option<AMQPFrame>>;

pub(crate) const NOT_FOUND: u16 = 404;
pub(crate) const PRECONDITION_FAILED: u16 = 406;
pub(crate) const CHANNEL_ERROR: u16 = 504;
pub(crate) const NOT_ALLOWED: u16 = 530;

/// All the transient data of a connection are stored here.
pub(crate) struct Connection {
    /// Unique ID of the connection.
    id: String,
    /// Context servers as a dependency holder, it keeps the references of the services.
    context: Arc<Mutex<Context>>,
    /// Opened channels by this connection.
    open_channels: HashMap<Channel, ()>,
    /// Declared exchanges by this connection.
    exchanges: HashMap<String, ExchangeCommandSink>,
    /// Declared queues by this connection.
    queues: HashMap<String, message::MessageChannel>,
    /// Consumed queues by this connection, consumer_tag -> queue_name
    consumed_queues: HashMap<String, String>,
    in_flight_contents: HashMap<Channel, PublishedContent>,
    outgoing: mpsc::Sender<AMQPFrame>,
}

#[derive(Debug)]
struct PublishedContent {
    channel: Channel,
    exchange: String,
    routing_key: String,
    mandatory: bool,
    immediate: bool,
    length: Option<u64>,
    content: Option<Vec<u8>>,
}

pub(crate) fn new(context: Arc<Mutex<Context>>, outgoing: mpsc::Sender<AMQPFrame>) -> Connection {
    Connection {
        id: Uuid::new_v4().to_hyphenated().to_string(),
        context: context,
        open_channels: HashMap::new(),
        exchanges: HashMap::new(),
        queues: HashMap::new(),
        consumed_queues: HashMap::new(),
        in_flight_contents: HashMap::new(),
        outgoing: outgoing,
    }
}

impl Connection {
    pub(crate) async fn connection_open(&self, channel: Channel, args: frame::ConnectionOpenArgs) -> MaybeFrame {
        if args.virtual_host != "/" {
            connection_error(NOT_ALLOWED, "Cannot connect to virtualhost", frame::CONNECTION_OPEN)
        } else {
            Ok(Some(frame::connection_open_ok(channel)))
        }
    }

    pub(crate) async fn connection_close(&self, _args: frame::ConnectionCloseArgs) -> MaybeFrame {
        // TODO cleanup
        let mut ctx = self.context.lock().await;
        for (consumer_tag, queue_name) in &self.consumed_queues {
            ctx.queues
                .cancel(queue_name.to_string(), consumer_tag.to_string())
                .await?;
        }

        Ok(Some(frame::connection_close_ok(0)))
    }

    pub(crate) async fn channel_open(&mut self, channel: Channel) -> MaybeFrame {
        if self.open_channels.contains_key(&channel) {
            channel_error(channel, CHANNEL_ERROR, "Channel already opened", frame::CHANNEL_OPEN)
        } else {
            self.open_channels.insert(channel, ());
            Ok(Some(frame::channel_open_ok(channel)))
        }
    }

    pub(crate) async fn channel_close(&mut self, channel: Channel, _args: frame::ChannelCloseArgs) -> MaybeFrame {
        let mut ctx = self.context.lock().await;
        ctx.exchanges.clone_connection("", "").await?;

        self.open_channels.remove(&channel);

        Ok(Some(frame::channel_close_ok(channel)))
    }

    pub(crate) async fn channel_close_ok(&mut self, channel: Channel) -> MaybeFrame {
        Ok(None)
    }

    pub(crate) async fn exchange_declare(&mut self, channel: Channel, args: frame::ExchangeDeclareArgs) -> MaybeFrame {
        let no_wait = args.flags.contains(frame::ExchangeDeclareFlags::NO_WAIT);
        let passive = args.flags.contains(frame::ExchangeDeclareFlags::PASSIVE);
        let exchange_name = args.exchange_name.clone();

        let mut ctx = self.context.lock().await;
        let result = ctx.exchanges.declare(args.into(), passive, &self.id).await;

        match result {
            Ok(ch) => {
                self.exchanges.insert(exchange_name.clone(), ch);

                if no_wait {
                    Ok(None)
                } else {
                    Ok(Some(frame::exchange_declare_ok(channel)))
                }
            }
            Err(e) => match e.downcast::<RuntimeError>() {
                Ok(mut rte) => {
                    rte.channel = channel;

                    Ok(Some(AMQPFrame::from(*rte)))
                }
                Err(e2) => Err(e2),
            },
        }
    }

    pub(crate) async fn exchange_delete(&mut self, channel: Channel, args: frame::ExchangeDeleteArgs) -> MaybeFrame {
        // TODO do the delete here
        Ok(Some(frame::exchange_delete_ok(channel)))
    }

    pub(crate) async fn queue_declare(&mut self, channel: Channel, args: frame::QueueDeclareArgs) -> MaybeFrame {
        let mut ctx = self.context.lock().await;
        ctx.queues.declare(args.name.clone()).await?;

        Ok(Some(frame::queue_declare_ok(channel, args.name, 0, 0)))
    }

    pub(crate) async fn queue_bind(&mut self, channel: Channel, args: frame::QueueBindArgs) -> MaybeFrame {
        let mut ctx = self.context.lock().await;

        if let Ok(ch) = ctx.queues.get_channel(args.queue_name).await {
            ctx.exchanges.bind_queue(args.exchange_name, ch).await?;
        } else {
        }

        Ok(Some(frame::queue_bind_ok(channel)))
    }

    pub(crate) async fn queue_delete(&mut self, channel: Channel, args: frame::QueueDeleteArgs) -> MaybeFrame {
        // TODO delete the queue
        Ok(Some(frame::queue_delete_ok(channel, 0)))
    }

    pub(crate) async fn queue_unbind(&mut self, channel: Channel, args: frame::QueueUnbindArgs) -> MaybeFrame {
        // TODO implement queue unbind
        Ok(Some(frame::queue_unbind_ok(channel)))
    }

    pub(crate) async fn basic_publish(&mut self, channel: Channel, args: frame::BasicPublishArgs) -> MaybeFrame {
        if !self.exchanges.contains_key(&args.exchange_name) {
            channel_error(channel, NOT_FOUND, "Exchange not found", frame::BASIC_PUBLISH)
        } else {
            // TODO check if there is in flight content in the channel -> error
            self.in_flight_contents.insert(
                channel,
                PublishedContent {
                    channel,
                    exchange: args.exchange_name,
                    routing_key: args.routing_key,
                    mandatory: args.flags.contains(frame::BasicPublishFlags::MANDATORY),
                    immediate: args.flags.contains(frame::BasicPublishFlags::IMMEDIATE),
                    length: None,
                    content: None,
                },
            );

            Ok(None)
        }
    }

    pub(crate) async fn basic_consume(&mut self, channel: Channel, args: frame::BasicConsumeArgs) -> MaybeFrame {
        let mut ctx = self.context.lock().await;
        ctx.queues
            .consume(args.queue.clone(), args.consumer_tag.clone(), self.outgoing.clone())
            .await?;
        self.consumed_queues.insert(args.consumer_tag.clone(), args.queue);

        Ok(Some(frame::basic_consume_ok(channel, &args.consumer_tag)))
    }

    pub(crate) async fn basic_cancel(&mut self, channel: Channel, args: frame::BasicCancelArgs) -> MaybeFrame {
        let mut ctx = self.context.lock().await;
        //ctx.queues.cancel(args.consumer_tag).await?;
        self.consumed_queues.remove(&args.consumer_tag);

        Ok(Some(frame::basic_cancel_ok(channel, &args.consumer_tag)))
    }

    pub(crate) async fn confirm_select(&mut self, channel: Channel, args: frame::ConfirmSelectArgs) -> MaybeFrame {
        Ok(None)
    }

    pub(crate) async fn receive_content_header(&mut self, header: frame::ContentHeaderFrame) -> MaybeFrame {
        // TODO collect info into a data struct
        if let Some(pc) = self.in_flight_contents.get_mut(&header.channel) {
            pc.length = Some(header.body_size);
        }

        Ok(None)
    }

    pub(crate) async fn receive_content_body(&mut self, body: frame::ContentBodyFrame) -> MaybeFrame {
        if let Some(pc) = self.in_flight_contents.remove(&body.channel) {
            let msg = message::Message {
                source_connection: self.id.clone(),
                channel: pc.channel,
                content: body.body,
                exchange: pc.exchange.clone(),
                routing_key: pc.routing_key,
                mandatory: pc.mandatory,
                immediate: pc.immediate,
            };

            match self.exchanges.get(&pc.exchange) {
                Some(ch) => {
                    ch.send(ExchangeCommand::Message(msg)).await?;
                    Ok(None)
                }
                None =>
                // TODO error, exchange cannot be found
                {
                    Ok(None)
                }
            }
        } else {
            Ok(None)
        }
    }
}

fn channel_error(channel: Channel, code: u16, text: &str, cm_id: u32) -> MaybeFrame {
    let (cid, mid) = frame::split_class_method(cm_id);

    Ok(Some(frame::channel_close(channel, code, text, cid, mid)))
}

fn connection_error(code: u16, text: &str, cm_id: u32) -> MaybeFrame {
    let (cid, mid) = frame::split_class_method(cm_id);

    Ok(Some(frame::connection_close(0, code, text, cid, mid)))
}
