use crate::client::state::{Connection, ConsumedQueue, MaybeFrame, PublishedContent};
use crate::client::{self, ChannelError};
use crate::exchange::handler::ExchangeCommand;
use crate::logerr;
use crate::message;
use crate::queue::handler as queue_handler;
use crate::queue::manager as qm;
use log::{error, warn};
use metalmq_codec::codec::Frame;
use metalmq_codec::frame::{self, Channel};
use tokio::time;

impl Connection {
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
                    ..Default::default()
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
            .position(|cq| cq.consumer_tag == args.consumer_tag)
        {
            let cq = &self.consumed_queues[pos];

            qm::cancel_consume(&self.qm, channel, &cq.queue_name, &args.consumer_tag).await?;

            self.consumed_queues.retain(|cq| cq.consumer_tag != args.consumer_tag);
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
            pc.content_type = header.content_type;
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
                content_type: pc.content_type,
            };

            match self.exchanges.get(&pc.exchange) {
                Some(ch) => {
                    // TODO is this the correct way of returning Err(_)
                    logerr!(
                        ch.send_timeout(ExchangeCommand::Message(msg), time::Duration::from_secs(1))
                            .await
                    );
                    Ok(None)
                }
                None => {
                    if msg.mandatory {
                        logerr!(message::send_basic_return(&msg, &self.outgoing).await);
                        Ok(None)
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
