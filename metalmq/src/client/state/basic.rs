use crate::client::state::{Connection, ConsumedQueue, PublishedContent};
use crate::client::{self, ChannelError};
use crate::exchange::handler::ExchangeCommand;
use crate::message;
use crate::queue::handler as queue_handler;
use crate::queue::manager::{self as qm, QueueCancelConsume, QueueConsumeCommand};
use crate::{logerr, Result};
use log::{error, warn};
use metalmq_codec::codec::Frame;
use metalmq_codec::frame::{self, Channel};
use tokio::time;

impl Connection {
    pub async fn basic_publish(&mut self, channel: Channel, args: frame::BasicPublishArgs) -> Result<()> {
        if !self.exchanges.contains_key(&args.exchange_name) {
            self.send_frame(client::channel_error_frame(
                channel,
                frame::BASIC_PUBLISH,
                ChannelError::NotFound,
                "Exchange not found",
            ))
            .await;

            // TODO
            // channel close and cleanup logic here!
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
        }

        Ok(())
    }

    pub async fn basic_consume(&mut self, channel: Channel, args: frame::BasicConsumeArgs) -> Result<()> {
        let queue_clone = args.queue.clone();
        let consumer_tag_clone = args.consumer_tag.clone();
        let cmd = QueueConsumeCommand {
            conn_id: self.id.clone(),
            channel,
            queue_name: queue_clone,
            consumer_tag: consumer_tag_clone,
            no_ack: args.flags.contains(frame::BasicConsumeFlags::NO_ACK),
            exclusive: args.flags.contains(frame::BasicConsumeFlags::EXCLUSIVE),
            outgoing: self.outgoing.clone(),
        };
        match qm::consume(&self.qm, cmd).await {
            Ok(queue_sink) => {
                let consumer_tag_clone = args.consumer_tag.clone();
                let queue_sink_clone = queue_sink.clone();

                self.consumed_queues.push(ConsumedQueue {
                    channel,
                    consumer_tag: args.consumer_tag.clone(),
                    queue_name: args.queue.clone(),
                    queue_sink,
                });

                self.send_sync_frame(Frame::Frame(frame::basic_consume_ok(channel, &args.consumer_tag)))
                    .await?;

                let start_deliver_cmd = queue_handler::QueueCommand::StartDelivering {
                    conn_id: self.id.clone(),
                    channel,
                    consumer_tag: consumer_tag_clone,
                };

                queue_sink_clone.send(start_deliver_cmd).await?;

                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub async fn basic_cancel(&mut self, channel: Channel, args: frame::BasicCancelArgs) -> Result<()> {
        if let Some(pos) = self
            .consumed_queues
            .iter()
            .position(|cq| cq.consumer_tag == args.consumer_tag)
        {
            let cq = &self.consumed_queues[pos];

            let cmd = QueueCancelConsume {
                channel: cq.channel,
                queue_name: cq.queue_name.clone(),
                consumer_tag: cq.consumer_tag.clone(),
            };
            qm::cancel_consume(&self.qm, cmd).await?;

            self.consumed_queues.retain(|cq| cq.consumer_tag != args.consumer_tag);

            self.send_frame(Frame::Frame(frame::basic_cancel_ok(channel, &args.consumer_tag)))
                .await?;
        } else {
            // TODO error: canceling consuming which didn't exist
            ()
        }

        Ok(())
    }

    pub async fn basic_ack(&mut self, channel: Channel, args: frame::BasicAckArgs) -> Result<()> {
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
                // TODO error? out of band ack?
                warn!("Acking a messages without consuming the queue {}", args.delivery_tag);
            }
        }

        Ok(())
    }

    pub async fn confirm_select(&mut self, channel: Channel, _args: frame::ConfirmSelectArgs) -> Result<()> {
        self.send_frame(Frame::Frame(frame::confirm_select_ok(channel))).await?;

        Ok(())
    }

    pub async fn receive_content_header(&mut self, header: frame::ContentHeaderFrame) -> Result<()> {
        // TODO collect info into a data struct
        if let Some(pc) = self.in_flight_contents.get_mut(&header.channel) {
            pc.length = Some(header.body_size);
            pc.content_type = header.content_type;
        }

        Ok(())
    }

    pub async fn receive_content_body(&mut self, body: frame::ContentBodyFrame) -> Result<()> {
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
                    Ok(())
                }
                None => {
                    if msg.mandatory {
                        logerr!(message::send_basic_return(&msg, &self.outgoing).await);
                        Ok(())
                    } else {
                        Ok(())
                    }
                }
            }
        } else {
            Ok(())
        }
    }
}
