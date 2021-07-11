use crate::client::{self, ChannelError, ConnectionError};
// Do we need to expose the messages of a 'process' or hide it in an erlang-style?
use crate::exchange::manager as em;
use crate::exchange::{handler::ExchangeCommand, handler::ExchangeCommandSink, handler::MessageSentResult};
use crate::message;
use crate::queue::handler as queue_handler;
use crate::queue::manager as qm;
use crate::{logerr, Context, ErrorScope, Result, RuntimeError};
use log::{error, info, trace, warn};
use metalmq_codec::codec::Frame;
use metalmq_codec::frame::{self, AMQPFrame, Channel};
use std::cmp::Ordering;
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};
use tokio::time;
use uuid::Uuid;

pub(crate) type MaybeFrame = Result<Option<Frame>>;

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
    qm: qm::QueueManagerSink,
    em: em::ExchangeManagerSink,
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
    outgoing: mpsc::Sender<Frame>,
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

pub(crate) fn new(context: Context, outgoing: mpsc::Sender<Frame>) -> Connection {
    let conn_id = Uuid::new_v4().to_hyphenated().to_string();

    info!("Client connected id = {}", conn_id);

    Connection {
        id: conn_id,
        qm: context.queue_manager,
        em: context.exchange_manager,
        open_channels: vec![],
        exchanges: HashMap::new(),
        auto_delete_exchanges: vec![],
        consumed_queues: vec![],
        in_flight_contents: HashMap::new(),
        outgoing,
    }
}

impl Connection {
    pub(crate) async fn connection_start_ok(&self, channel: Channel, args: frame::ConnectionStartOkArgs) -> MaybeFrame {
        let mut authenticated = false;

        if args.mechanism.cmp(&"PLAIN".to_string()) == Ordering::Equal {
            let mut it = args.response.as_bytes().split(|b| b == &0u8);
            it.next();
            let username = it.next();
            let password = it.next();

            trace!("User {:?} Pass {:?}", username, password);

            if let (Some(b"guest"), Some(b"guest")) = (username, password) {
                authenticated = true;
            }
        }

        match authenticated {
            true => Ok(Some(Frame::Frame(frame::connection_tune(channel)))),
            false => client::connection_error(
                0u32,
                ConnectionError::AccessRefused,
                "Username and password are incorrect",
            ),
        }
    }

    pub(crate) async fn connection_open(&self, channel: Channel, args: frame::ConnectionOpenArgs) -> MaybeFrame {
        if args.virtual_host != "/" {
            client::connection_error(
                frame::CONNECTION_OPEN,
                ConnectionError::NotAllowed,
                "Cannot connect to virtualhost",
            )
        } else {
            Ok(Some(Frame::Frame(frame::connection_open_ok(channel))))
        }
    }

    pub(crate) async fn connection_close(&self, _args: frame::ConnectionCloseArgs) -> MaybeFrame {
        info!("Connection {} is being closed", self.id);

        // TODO cleanup
        //   - consume handler -> remove as consumer, auto delete queues are deleted when there are
        //     no consumers there
        //   - exchange handler -> deregister (auto-delete exchange)
        //   - queues -> delete the exclusive queues
        for cq in &self.consumed_queues {
            trace!("Cleaning up consumers {:?}", cq);

            if let Err(e) = qm::cancel_consume(&self.qm, cq.channel, &cq.queue_name, &cq.consumer_tag).await {
                error!("Err {:?}", e);
            }
        }

        Ok(Some(Frame::Frame(frame::connection_close_ok(0))))
    }

    pub(crate) async fn channel_open(&mut self, channel: Channel) -> MaybeFrame {
        if self.open_channels.iter().any(|&c| c == channel) {
            client::connection_error(
                frame::CHANNEL_OPEN,
                ConnectionError::ChannelError,
                "Channel already opened",
            )
        } else {
            self.open_channels.push(channel);
            Ok(Some(Frame::Frame(frame::channel_open_ok(channel))))
        }
    }

    pub(crate) async fn channel_close(&mut self, channel: Channel, _args: frame::ChannelCloseArgs) -> MaybeFrame {
        // TODO close all exchanges and queues it needs to.
        if !self.auto_delete_exchanges.is_empty() {
            for exchange_name in &self.auto_delete_exchanges {
                // TODO this is bad here, we hold the locks until the exchanges are not deleted
                // I don't know if await yield release that locks but I doubt it.
                logerr!(em::delete_exchange(&self.em, channel, exchange_name).await);
            }
        }

        self.consumed_queues.retain(|cq| cq.channel != channel);

        self.open_channels.retain(|c| c != &channel);

        Ok(Some(Frame::Frame(frame::channel_close_ok(channel))))
    }

    pub(crate) async fn channel_close_ok(&mut self, channel: Channel) -> MaybeFrame {
        self.consumed_queues.retain(|cq| cq.channel != channel);

        Ok(None)
    }

    pub(crate) async fn exchange_declare(&mut self, channel: Channel, args: frame::ExchangeDeclareArgs) -> MaybeFrame {
        let no_wait = args.flags.contains(frame::ExchangeDeclareFlags::NO_WAIT);
        let passive = args.flags.contains(frame::ExchangeDeclareFlags::PASSIVE);
        let exchange_name = args.exchange_name.clone();

        let result = em::declare_exchange(&self.em, channel, args.into(), passive).await;

        match result {
            Ok(ch) => {
                self.exchanges.insert(exchange_name.clone(), ch);

                if no_wait {
                    Ok(None)
                } else {
                    Ok(Some(Frame::Frame(frame::exchange_declare_ok(channel))))
                }
            }
            // TODO is it automatic now?
            Err(e) => match e.downcast::<RuntimeError>() {
                Ok(mut rte) => match rte.scope {
                    ErrorScope::Connection => Ok(Some(Frame::Frame(AMQPFrame::from(*rte)))),
                    ErrorScope::Channel => {
                        rte.channel = channel;
                        Ok(Some(Frame::Frame(AMQPFrame::from(*rte))))
                    }
                },
                Err(e2) => Err(e2),
            },
        }
    }

    pub(crate) async fn exchange_delete(&mut self, channel: Channel, args: frame::ExchangeDeleteArgs) -> MaybeFrame {
        em::delete_exchange(&self.em, channel, &args.exchange_name).await?;

        self.exchanges.remove(&args.exchange_name);

        Ok(Some(Frame::Frame(frame::exchange_delete_ok(channel))))
    }

    pub(crate) async fn queue_declare(&mut self, channel: Channel, args: frame::QueueDeclareArgs) -> MaybeFrame {
        let queue_name = args.name.clone();

        qm::declare_queue(&self.qm, args.into(), &self.id, channel).await?;

        Ok(Some(Frame::Frame(frame::queue_declare_ok(channel, queue_name, 0, 0))))
    }

    pub(crate) async fn queue_bind(&mut self, channel: Channel, args: frame::QueueBindArgs) -> MaybeFrame {
        match qm::get_command_sink(&self.qm, channel, &args.queue_name).await {
            Ok(sink) => {
                em::bind_queue(
                    &self.em,
                    channel,
                    &args.exchange_name,
                    &args.queue_name,
                    &args.routing_key,
                    sink,
                )
                .await?;

                Ok(Some(Frame::Frame(frame::queue_bind_ok(channel))))
            }
            Err(_) => client::channel_error(channel, frame::QUEUE_BIND, ChannelError::NotFound, "Exchange not found"),
        }
    }

    pub(crate) async fn queue_delete(&mut self, channel: Channel, args: frame::QueueDeleteArgs) -> MaybeFrame {
        // TODO delete the queue
        Ok(Some(Frame::Frame(frame::queue_delete_ok(channel, 0))))
    }

    pub(crate) async fn queue_unbind(&mut self, channel: Channel, args: frame::QueueUnbindArgs) -> MaybeFrame {
        em::unbind_queue(
            &self.em,
            channel,
            &args.exchange_name,
            &args.queue_name,
            &args.routing_key,
        )
        .await?;

        Ok(Some(Frame::Frame(frame::queue_unbind_ok(channel))))
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

            Ok(None)
        }
    }

    pub(crate) async fn basic_consume(&mut self, channel: Channel, args: frame::BasicConsumeArgs) -> MaybeFrame {
        match qm::consume(
            &self.qm,
            &self.id,
            channel,
            &args.queue,
            &args.consumer_tag,
            args.flags.contains(frame::BasicConsumeFlags::NO_ACK),
            args.flags.contains(frame::BasicConsumeFlags::EXCLUSIVE),
            self.outgoing.clone(),
        )
        .await
        {
            Ok(queue_sink) => {
                self.consumed_queues.push(ConsumedQueue {
                    channel,
                    consumer_tag: args.consumer_tag.clone(),
                    queue_name: args.queue.clone(),
                    queue_sink,
                });
                Ok(Some(Frame::Frame(frame::basic_consume_ok(channel, &args.consumer_tag))))
            }
            Err(e) => Err(e),
        }
    }

    pub(crate) async fn basic_cancel(&mut self, channel: Channel, args: frame::BasicCancelArgs) -> MaybeFrame {
        if let Some(pos) = self
            .consumed_queues
            .iter()
            .position(|cq| cq.consumer_tag.cmp(&args.consumer_tag) == std::cmp::Ordering::Equal)
        {
            let cq = &self.consumed_queues[pos];

            qm::cancel_consume(&self.qm, channel, &cq.queue_name, &args.consumer_tag).await?;

            self.consumed_queues
                .retain(|cq| cq.consumer_tag.cmp(&args.consumer_tag) != std::cmp::Ordering::Equal);
            Ok(Some(Frame::Frame(frame::basic_cancel_ok(channel, &args.consumer_tag))))
        } else {
            // TODO error: canceling consuming which didn't exist
            Ok(None)
        }
    }

    pub(crate) async fn basic_ack(&mut self, channel: Channel, args: frame::BasicAckArgs) -> MaybeFrame {
        match self.consumed_queues.iter().position(|cq| cq.channel == channel) {
            Some(p) => {
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
            None => {
                warn!("Acking a messages without consuming the queue {}", args.delivery_tag);
            }
        }

        Ok(None)
    }

    pub(crate) async fn confirm_select(&mut self, channel: Channel, _args: frame::ConfirmSelectArgs) -> MaybeFrame {
        Ok(Some(Frame::Frame(frame::confirm_select_ok(channel))))
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
                    // TODO what happens if we cannot route the message to an existing exchange?
                    // Probably we need to send back an error if it is a mandatory message
                    let (tx, rx) = oneshot::channel();
                    ch.send_timeout(ExchangeCommand::Message(msg, tx), time::Duration::from_secs(1))
                        .await?;

                    match rx.await {
                        Ok(MessageSentResult::None) => Ok(None),
                        Ok(MessageSentResult::MessageNotRouted(original_message)) => {
                            send_basic_return(original_message)
                        }
                        Err(e) => {
                            error!("Receiving response from exchange {:?}", e);
                            Ok(None)
                        }
                    }
                }
                None => {
                    if msg.mandatory {
                        send_basic_return(msg)
                    } else {
                        Ok(None)
                    }
                }
            }
        } else {
            Ok(None)
        }
    }
}

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

    Ok(Some(Frame::Frames(frames)))
}
