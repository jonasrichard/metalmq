use crate::client::{self, ChannelError, ConnectionError};
use crate::exchange::{handler::ExchangeCommand, handler::ExchangeCommandSink, handler::MessageSentResult};
use crate::message;
use crate::queue::handler as queue_handler;
use crate::{Context, ErrorScope, Result, RuntimeError};
use log::{error, trace};
use metalmq_codec::frame::{self, AMQPFrame, Channel};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::time;
use uuid::Uuid;

/// Response of a handler function
pub(crate) enum FrameResponse {
    None,
    Frame(AMQPFrame),
    Frames(Vec<AMQPFrame>),
}

pub(crate) type MaybeFrame = Result<FrameResponse>;

#[derive(Debug)]
struct ConsumedQueue {
    channel: Channel,
    queue_name: String,
    consumer_tag: String,
    queue_sink: queue_handler::QueueCommandSink,
}

/// All the transient data of a connection are stored here.
pub(crate) struct Connection {
    /// Unique ID of the connection.
    id: String,
    /// Context servers as a dependency holder, it keeps the references of the services.
    context: Arc<Mutex<Context>>,
    /// Opened channels by this connection.
    open_channels: Vec<Channel>,
    /// Declared exchanges by this connection.
    exchanges: HashMap<String, ExchangeCommandSink>,
    /// Exchanges which declared by this channel as auto-delete
    auto_delete_exchanges: Vec<String>,
    /// Consumed queues by this connection
    consumed_queues: Vec<ConsumedQueue>,
    /// Incoming messages come in different messages, we need to collect their properties
    in_flight_contents: HashMap<Channel, PublishedContent>,
    /// Sink for AMQP frames toward the client
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
        context,
        open_channels: vec![],
        exchanges: HashMap::new(),
        auto_delete_exchanges: vec![],
        consumed_queues: vec![],
        in_flight_contents: HashMap::new(),
        outgoing,
    }
}

impl Connection {
    pub(crate) async fn connection_open(&self, channel: Channel, args: frame::ConnectionOpenArgs) -> MaybeFrame {
        if args.virtual_host != "/" {
            client::connection_error(
                frame::CONNECTION_OPEN,
                ConnectionError::NotAllowed,
                "Cannot connect to virtualhost",
            )
        } else {
            Ok(FrameResponse::Frame(frame::connection_open_ok(channel)))
        }
    }

    pub(crate) async fn connection_close(&self, _args: frame::ConnectionCloseArgs) -> MaybeFrame {
        // TODO cleanup
        //   - consume handler -> remove as consumer, auto delete queues are deleted when there are
        //     no consumers there
        //   - exchange handler -> deregister (auto-delete exchange)
        //   - queues -> delete the exclusive queues
        let mut ctx = self.context.lock().await;
        for cq in &self.consumed_queues {
            trace!("Cleaning up consumers {:?}", cq);

            ctx.queues
                .cancel(cq.queue_name.clone(), cq.consumer_tag.clone())
                .await?;
        }

        Ok(FrameResponse::Frame(frame::connection_close_ok(0)))
    }

    pub(crate) async fn channel_open(&mut self, channel: Channel) -> MaybeFrame {
        if self.open_channels.iter().position(|c| c == &channel).is_some() {
            client::connection_error(
                frame::CHANNEL_OPEN,
                ConnectionError::ChannelError,
                "Channel already opened",
            )
        } else {
            self.open_channels.push(channel);
            Ok(FrameResponse::Frame(frame::channel_open_ok(channel)))
        }
    }

    pub(crate) async fn channel_close(&mut self, channel: Channel, _args: frame::ChannelCloseArgs) -> MaybeFrame {
        // TODO close all exchanges and queues it needs to.
        if !self.auto_delete_exchanges.is_empty() {
            let mut ctx = self.context.lock().await;

            for exchange_name in &self.auto_delete_exchanges {
                // TODO this is bad here, we hold the locks until the exchanges are not deleted
                // I don't know if await yield release that locks but I doubt it.
                ctx.exchanges.delete_exchange(exchange_name).await;
            }

            drop(ctx);
        }

        self.consumed_queues.retain(|cq| cq.channel != channel);

        self.open_channels.retain(|c| c != &channel);

        Ok(FrameResponse::Frame(frame::channel_close_ok(channel)))
    }

    pub(crate) async fn channel_close_ok(&mut self, channel: Channel) -> MaybeFrame {
        self.consumed_queues.retain(|cq| cq.channel != channel);

        Ok(FrameResponse::None)
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
                    Ok(FrameResponse::None)
                } else {
                    Ok(FrameResponse::Frame(frame::exchange_declare_ok(channel)))
                }
            }
            Err(e) => match e.downcast::<RuntimeError>() {
                Ok(mut rte) => match rte.scope {
                    ErrorScope::Connection => Ok(FrameResponse::Frame(AMQPFrame::from(*rte))),
                    ErrorScope::Channel => {
                        rte.channel = channel;
                        Ok(FrameResponse::Frame(AMQPFrame::from(*rte)))
                    }
                },
                Err(e2) => Err(e2),
            },
        }
    }

    pub(crate) async fn exchange_delete(&mut self, channel: Channel, args: frame::ExchangeDeleteArgs) -> MaybeFrame {
        // TODO do the delete here
        Ok(FrameResponse::Frame(frame::exchange_delete_ok(channel)))
    }

    pub(crate) async fn queue_declare(&mut self, channel: Channel, args: frame::QueueDeclareArgs) -> MaybeFrame {
        let mut ctx = self.context.lock().await;

        ctx.queues.declare(args.name.clone()).await?;

        Ok(FrameResponse::Frame(frame::queue_declare_ok(channel, args.name, 0, 0)))
    }

    pub(crate) async fn queue_bind(&mut self, channel: Channel, args: frame::QueueBindArgs) -> MaybeFrame {
        let mut ctx = self.context.lock().await;

        match ctx.queues.get_command_sink(&args.queue_name).await {
            Ok(sink) => {
                ctx.exchanges
                    .bind_queue(&args.exchange_name, &args.queue_name, &args.routing_key, sink)
                    .await?;

                Ok(FrameResponse::Frame(frame::queue_bind_ok(channel)))
            }
            Err(e) => client::channel_error(channel, frame::QUEUE_BIND, ChannelError::NotFound, "Exchange not found"),
        }
    }

    pub(crate) async fn queue_delete(&mut self, channel: Channel, args: frame::QueueDeleteArgs) -> MaybeFrame {
        // TODO delete the queue
        Ok(FrameResponse::Frame(frame::queue_delete_ok(channel, 0)))
    }

    pub(crate) async fn queue_unbind(&mut self, channel: Channel, args: frame::QueueUnbindArgs) -> MaybeFrame {
        // TODO implement queue unbind
        Ok(FrameResponse::Frame(frame::queue_unbind_ok(channel)))
    }

    pub(crate) async fn basic_publish(&mut self, channel: Channel, args: frame::BasicPublishArgs) -> MaybeFrame {
        if !self.exchanges.contains_key(&args.exchange_name) {
            client::channel_error(
                channel,
                frame::BASIC_PUBLISH,
                ChannelError::NotFound,
                "Exchange not found",
            )
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

            Ok(FrameResponse::None)
        }
    }

    pub(crate) async fn basic_consume(&mut self, channel: Channel, args: frame::BasicConsumeArgs) -> MaybeFrame {
        let mut ctx = self.context.lock().await;
        ctx.queues
            .consume(
                args.queue.clone(),
                args.consumer_tag.clone(),
                args.flags.contains(frame::BasicConsumeFlags::NO_ACK),
                self.outgoing.clone(),
            )
            .await?;

        match ctx.queues.get_command_sink(&args.queue).await {
            Ok(queue_channel) => {
                self.consumed_queues.push(ConsumedQueue {
                    channel,
                    consumer_tag: args.consumer_tag.clone(),
                    queue_name: args.queue.clone(),
                    queue_sink: queue_channel,
                });
                Ok(FrameResponse::Frame(frame::basic_consume_ok(
                    channel,
                    &args.consumer_tag,
                )))
            }
            Err(_) => client::channel_error(channel, frame::BASIC_CONSUME, ChannelError::NotFound, "Queue not found"),
        }
    }

    pub(crate) async fn basic_cancel(&mut self, channel: Channel, args: frame::BasicCancelArgs) -> MaybeFrame {
        if let Some(pos) = self
            .consumed_queues
            .iter()
            .position(|cq| cq.consumer_tag.cmp(&args.consumer_tag) == std::cmp::Ordering::Equal)
        {
            let mut ctx = self.context.lock().await;

            let cq = self.consumed_queues.get(pos).unwrap();

            ctx.queues
                .cancel(cq.queue_name.clone(), cq.consumer_tag.clone())
                .await?;

            self.consumed_queues
                .retain(|cq| cq.consumer_tag.cmp(&args.consumer_tag) != std::cmp::Ordering::Equal);
            Ok(FrameResponse::Frame(frame::basic_cancel_ok(
                channel,
                &args.consumer_tag,
            )))
        } else {
            // TODO error: canceling consuming which didn't exist
            Ok(FrameResponse::None)
        }
    }

    pub(crate) async fn basic_ack(&mut self, channel: Channel, args: frame::BasicAckArgs) -> MaybeFrame {
        if let Some(p) = self.consumed_queues.iter().position(|cq| cq.channel == channel) {
            let cq = self.consumed_queues.get(p).unwrap();

            cq.queue_sink
                .send_timeout(
                    queue_handler::QueueCommand::AckMessage {
                        consumer_tag: cq.consumer_tag.clone(),
                        delivery_tag: args.delivery_tag,
                    },
                    time::Duration::from_secs(1),
                )
                .await?;
        }

        Ok(FrameResponse::None)
    }

    pub(crate) async fn confirm_select(&mut self, channel: Channel, args: frame::ConfirmSelectArgs) -> MaybeFrame {
        Ok(FrameResponse::Frame(frame::confirm_select_ok(channel)))
    }

    pub(crate) async fn receive_content_header(&mut self, header: frame::ContentHeaderFrame) -> MaybeFrame {
        // TODO collect info into a data struct
        if let Some(pc) = self.in_flight_contents.get_mut(&header.channel) {
            pc.length = Some(header.body_size);
        }

        Ok(FrameResponse::None)
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
                    // TODO what happens if we cannot route the message to an existing exchange?
                    // Probably we need to send back an error if it is a mandatory message
                    let (tx, rx) = oneshot::channel();
                    ch.send_timeout(ExchangeCommand::Message(msg, tx), time::Duration::from_secs(1))
                        .await?;

                    match rx.await {
                        Ok(MessageSentResult::None) => Ok(FrameResponse::None),
                        Ok(MessageSentResult::MessageNotRouted(original_message)) => {
                            send_basic_return(original_message)
                        }
                        Err(e) => {
                            error!("Receiving response from exchange {:?}", e);
                            Ok(FrameResponse::None)
                        }
                    }
                }
                None => {
                    if msg.mandatory {
                        send_basic_return(msg)
                    } else {
                        Ok(FrameResponse::None)
                    }
                }
            }
        } else {
            Ok(FrameResponse::None)
        }
    }
}

//fn channel_error(channel: Channel, code: u16, text: &str, cm_id: u32) -> MaybeFrame {
//    let (cid, mid) = frame::split_class_method(cm_id);
//
//    Ok(FrameResponse::Frame(frame::channel_close(
//        channel, code, text, cid, mid,
//    )))
//}
//
//fn connection_error(code: u16, text: &str, cm_id: u32) -> MaybeFrame {
//    let (cid, mid) = frame::split_class_method(cm_id);
//
//    Ok(FrameResponse::Frame(frame::connection_close(0, code, text, cid, mid)))
//}

fn send_basic_return(message: message::Message) -> MaybeFrame {
    let mut frames = message::message_to_content_frames(&message);

    frames.insert(
        0,
        frame::basic_return(
            message.channel,
            312,
            "NO_ROUTE",
            &message.exchange,
            &message.routing_key,
        ),
    );

    frames.push(frame::basic_ack(message.channel, 1u64, false));

    Ok(FrameResponse::Frames(frames))
}
