use crate::client::state::{Connection, ConsumedQueue, PublishedContent};
use crate::client::ChannelError;
use crate::exchange::handler::ExchangeCommand;
use crate::queue::handler as queue_handler;
use crate::queue::manager::{self as qm, QueueCancelConsume, QueueConsumeCommand};
use crate::{client, message};
use crate::{handle_error, logerr, Result};
use log::{error, info, warn};
use metalmq_codec::codec::Frame;
use metalmq_codec::frame::{self, Channel};
use std::sync::Arc;
use tokio::time;

impl Connection {
    pub async fn basic_publish(&mut self, channel: Channel, args: frame::BasicPublishArgs) -> Result<()> {
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

        let queue_sink = handle_error!(self, qm::consume(&self.qm, cmd).await).unwrap();

        let consumer_tag_clone = args.consumer_tag.clone();
        let queue_sink_clone = queue_sink.clone();

        self.consumed_queues.insert(
            channel,
            ConsumedQueue {
                consumer_tag: args.consumer_tag.clone(),
                queue_name: args.queue.clone(),
                queue_sink,
            },
        );

        self.send_frame(Frame::Frame(frame::basic_consume_ok(channel, &args.consumer_tag)))
            .await?;

        let start_deliver_cmd = queue_handler::QueueCommand::StartDelivering {
            consumer_tag: consumer_tag_clone,
        };

        queue_sink_clone.send(start_deliver_cmd).await?;

        Ok(())
    }

    pub async fn basic_cancel(&mut self, channel: Channel, args: frame::BasicCancelArgs) -> Result<()> {
        if let Some(cq) = self.consumed_queues.remove(&channel) {
            let cmd = QueueCancelConsume {
                channel,
                queue_name: cq.queue_name.clone(),
                consumer_tag: cq.consumer_tag.clone(),
            };
            qm::cancel_consume(&self.qm, cmd).await?;

            self.send_frame(Frame::Frame(frame::basic_cancel_ok(channel, &args.consumer_tag)))
                .await?;
        } else {
            // TODO error: canceling consuming which didn't exist
        }

        Ok(())
    }

    /// Handles Ack coming from client.
    ///
    /// A message can be acked more than once. If a non-delivered message is acked, a channel
    /// exception will be raised.
    pub async fn basic_ack(&mut self, channel: Channel, args: frame::BasicAckArgs) -> Result<()> {
        // TODO check if only delivered messages are acked, even multiple times
        match self.consumed_queues.get(&channel) {
            Some(cq) => {
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
        if let Some(ch) = self.open_channels.get_mut(&channel) {
            ch.confirm_mode = true;

            self.send_frame(Frame::Frame(frame::confirm_select_ok(channel))).await?;
        }

        Ok(())
    }

    pub async fn receive_content_header(&mut self, header: frame::ContentHeaderFrame) -> Result<()> {
        // TODO collect info into a data struct
        if let Some(pc) = self.in_flight_contents.get_mut(&header.channel) {
            pc.content_header = header;
        }

        Ok(())
    }

    pub async fn receive_content_body(&mut self, body: frame::ContentBodyFrame) -> Result<()> {
        info!("Message length {}", body.body.len());

        if let Some(mut pc) = self.in_flight_contents.remove(&body.channel) {
            let channel = body.channel;

            pc.body_size += body.body.len() as usize;
            if pc.body_size > 131_072 {
                error!("Content is too large {}", pc.body_size);

                handle_error!(
                    self,
                    client::channel_error::<()>(
                        channel,
                        frame::BASIC_PUBLISH,
                        ChannelError::ContentTooLarge,
                        "Body is too long",
                    )
                )
                .unwrap();
            }

            pc.content_bodies.push(body);

            if pc.body_size < pc.content_header.body_size as usize {
                self.in_flight_contents.insert(channel, pc);

                return Ok(());
            } else {
                let mut message_body = vec![];

                for cb in pc.content_bodies {
                    message_body.extend(cb.body);
                }

                let msg = message::Message {
                    source_connection: self.id.clone(),
                    channel: pc.channel,
                    exchange: pc.exchange.clone(),
                    routing_key: pc.routing_key,
                    mandatory: pc.mandatory,
                    immediate: pc.immediate,
                    content: message::MessageContent {
                        class_id: pc.content_header.class_id,
                        weight: pc.content_header.weight,
                        body: message_body,
                        body_size: pc.content_header.body_size,
                        prop_flags: pc.content_header.prop_flags,
                        content_encoding: pc.content_header.content_encoding,
                        content_type: pc.content_header.content_type,
                        delivery_mode: pc.content_header.delivery_mode,
                        message_id: pc.content_header.message_id,
                        timestamp: pc.content_header.timestamp,
                        headers: pc.content_header.headers,
                        // TODO copy all the message properties
                        ..Default::default()
                    },
                };

                // FIXME this logic
                //
                // We need to deal with mandatory if the channel is in confirm mode.
                // If confirm is on, message sending to exchange will be a blocking process because
                // based on the message properties, we may need to wait for the message to be sent to
                // the consumer.
                // Of course, here we cannot block/yield that much, so we need to keep track of the
                // messages on what we are waiting confirm, and the message needs to have a oneshot
                // channel with which the exchange/queue/consumer can tell us that it managed to
                // process the message.
                //
                // We can even have two types of message command as ExchangeCommand: one which is async
                // and one which waits for confirmation.
                match self.exchanges.get(&pc.exchange) {
                    Some(ch) => {
                        // FIXME again, this is not good, we shouldn't clone outgoing channels all the
                        // time
                        let cmd = ExchangeCommand::Message {
                            message: msg,
                            outgoing: self.outgoing.clone(),
                        };
                        // TODO is this the correct way of returning Err(_)
                        logerr!(ch.send_timeout(cmd, time::Duration::from_secs(1)).await);
                    }
                    None => {
                        if msg.mandatory {
                            logerr!(message::send_basic_return(Arc::new(msg), &self.outgoing).await);
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
