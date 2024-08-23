use std::{sync::Arc, time::Duration};

use log::{error, warn};
use metalmq_codec::{
    codec::Frame,
    frame::{self, Channel},
};
use tokio::sync::oneshot;

use crate::{
    client::{self, channel_error, connection_error, state::Connection, ChannelError, ConnectionError},
    exchange::handler::ExchangeCommand,
    handle_error, logerr,
    message::{self, Message},
    queue::{
        handler as queue_handler,
        manager::{self as qm, QueueCancelConsume, QueueConsumeCommand},
    },
    Result,
};

use super::{ActivelyConsumedQueue, PassivelyConsumedQueue, PublishedContent};

impl Connection {
    pub async fn basic_publish(&mut self, channel: Channel, args: frame::BasicPublishArgs) -> Result<()> {
        // Ensure that the exchange command sink is cached
        if !self.exchanges.contains_key(&args.exchange_name) {
            let exchange_sink = self.find_exchange(channel, &args.exchange_name).await?;

            self.exchanges.insert(args.exchange_name.clone(), exchange_sink);
        }

        // If we have an in-flight half content (no body), send back an unexpected frame error.
        if self.in_flight_contents.contains_key(&channel) {
            return connection_error(
                frame::BASIC_PUBLISH,
                ConnectionError::UnexpectedFrame,
                "Unexpected Basic.Publish during receiving message header and body",
            );
        }

        // Start collecting the published message
        self.in_flight_contents.insert(
            channel,
            PublishedContent {
                source_connection: self.id.clone(),
                channel,
                exchange: args.exchange_name.clone(),
                routing_key: args.routing_key.clone(),
                mandatory: args.is_mandatory(),
                immediate: args.is_immediate(),
                method_frame_class_id: frame::BASIC_PUBLISH,
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
            frame_size: self.frame_max,
        };

        let queue_sink = handle_error!(self, qm::consume(&self.qm, cmd).await).unwrap();

        let consumer_tag_clone = args.consumer_tag.clone();
        let queue_sink_clone = queue_sink.clone();

        self.consumed_queues.insert(
            channel,
            ActivelyConsumedQueue {
                consumer_tag: args.consumer_tag.clone(),
                queue_name: args.queue.clone(),
                queue_sink,
            },
        );

        self.send_frame(Frame::Frame(
            frame::BasicConsumeOkArgs::new(&args.consumer_tag).frame(channel),
        ))
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
        }

        self.send_frame(Frame::Frame(
            frame::BasicCancelOkArgs::new(&args.consumer_tag).frame(channel),
        ))
        .await
    }

    /// Handles Ack coming from client.
    ///
    /// A message can be acked more than once. If a non-delivered message is acked, a channel
    /// exception will be raised.
    pub async fn basic_ack(&mut self, channel: Channel, args: frame::BasicAckArgs) -> Result<()> {
        // TODO check if only delivered messages are acked, even multiple times
        match self.consumed_queues.get(&channel) {
            Some(cq) => {
                let (tx, rx) = oneshot::channel();

                // TODO why we need here the timeout?
                cq.queue_sink
                    .send_timeout(
                        queue_handler::QueueCommand::AckMessage(queue_handler::AckCmd {
                            channel,
                            consumer_tag: cq.consumer_tag.clone(),
                            delivery_tag: args.delivery_tag,
                            multiple: args.multiple,
                            result: tx,
                        }),
                        Duration::from_secs(1),
                    )
                    .await?;

                rx.await.unwrap()?
            }
            None => match self.passively_consumed_queues.get(&channel) {
                Some(pq) => {
                    let (tx, rx) = oneshot::channel();

                    pq.queue_sink
                        .send(queue_handler::QueueCommand::AckMessage(queue_handler::AckCmd {
                            channel,
                            consumer_tag: pq.consumer_tag.clone(),
                            delivery_tag: args.delivery_tag,
                            multiple: args.multiple,
                            result: tx,
                        }))
                        .await?;

                    handle_error!(self, rx.await.unwrap()).unwrap();
                }
                None => {
                    warn!("Basic.Ack arrived without consuming the queue");
                }
            },
        }

        Ok(())
    }

    pub async fn basic_get(&mut self, channel: Channel, args: frame::BasicGetArgs) -> Result<()> {
        let mut queue = self.passively_consumed_queues.get(&channel);

        if queue.is_none() {
            let sink = qm::get_command_sink(
                &self.qm,
                qm::GetQueueSinkQuery {
                    channel,
                    queue_name: args.queue.clone(),
                },
            )
            .await;

            if sink.is_err() {
                return channel_error(
                    channel,
                    frame::BASIC_GET,
                    ChannelError::NotFound,
                    &format!("Queue {} not found", args.queue),
                );
            }

            let sink = sink.unwrap();
            let (tx, rx) = oneshot::channel();

            sink.send(queue_handler::QueueCommand::PassiveConsume(
                queue_handler::PassiveConsumeCmd {
                    conn_id: self.id.clone(),
                    channel,
                    sink: self.outgoing.clone(),
                    frame_size: self.frame_max,
                    result: tx,
                },
            ))
            .await
            .unwrap();

            rx.await.unwrap().unwrap();

            let pq = PassivelyConsumedQueue {
                queue_name: args.queue,
                consumer_tag: format!("{}-{}", self.id, channel),
                delivery_tag: 1u64,
                queue_sink: sink,
            };

            self.passively_consumed_queues.insert(channel, pq);

            queue = self.passively_consumed_queues.get(&channel);
        }

        let queue = queue.unwrap();

        let (tx, rx) = oneshot::channel();

        queue
            .queue_sink
            .send(queue_handler::QueueCommand::Get(queue_handler::GetCmd {
                conn_id: self.id.clone(),
                channel,
                no_ack: args.no_ack,
                result: tx,
            }))
            .await
            .unwrap();

        // TODO handle error here
        rx.await.unwrap()
    }

    pub async fn basic_reject(&mut self, _channel: Channel, _args: frame::BasicRejectArgs) -> Result<()> {
        // TODO reject passively and actively listened queue's messages
        Ok(())
    }

    pub async fn confirm_select(&mut self, channel: Channel, _args: frame::ConfirmSelectArgs) -> Result<()> {
        self.next_confirm_delivery_tag.insert(channel, 1u64);

        self.send_frame(Frame::Frame(frame::confirm_select_ok(channel))).await?;

        Ok(())
    }

    pub async fn receive_content_header(&mut self, header: frame::ContentHeaderFrame) -> Result<()> {
        // TODO collect info into a data struct
        // TODO if body_size is 0, there won't be content body frame, so we need to send Message
        // now!
        if let Some(pc) = self.in_flight_contents.get_mut(&header.channel) {
            pc.handle_content_header(header)?;
        }

        Ok(())
    }

    pub async fn receive_content_body(&mut self, body: frame::ContentBodyFrame) -> Result<()> {
        let channel = body.channel;

        if body.body.len() > 131_072 {
            error!("Content is too large {}", body.body.len());

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

        if let Some(mut pc) = self.in_flight_contents.remove(&body.channel) {
            pc.handle_content_body(body)?;

            if pc.body_size < pc.content_header.body_size as usize {
                self.in_flight_contents.insert(channel, pc);

                return Ok(());
            } else {
                let msg: Message = pc.into();

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

                // FIXME we need to separate the exchange lookup logic
                // A client can send a message to the default exchange which is empty string, so
                // in that case the exchange won't be in the 'cache'. Also a client can send
                // message without declaring an exchange, so we need to ask exchange manager if
                // this exchange exists. Of course we can cache the exchanges, but in that case if
                // anyone deletes the exchange the client state needs to be notified.
                //
                // FIXME also message sending should be somewhere else in order to be testable
                match self.exchanges.get(&msg.exchange) {
                    Some(ch) => {
                        // If message is mandatory or the channel is in confirm mode we can expect
                        // returned message.
                        let (tx, rx) = match msg.mandatory || self.next_confirm_delivery_tag.contains_key(&channel) {
                            false => (None, None),
                            true => {
                                let (tx, rx) = oneshot::channel();
                                (Some(tx), Some(rx))
                            }
                        };

                        let cmd = ExchangeCommand::Message {
                            message: msg,
                            returned: tx,
                        };

                        // TODO is this the correct way of returning Err(_)
                        logerr!(ch.send_timeout(cmd, Duration::from_secs(1)).await);

                        if let Some(rx) = rx {
                            match rx.await.unwrap() {
                                Some(returned_message) => {
                                    message::send_basic_return(returned_message, self.frame_max, &self.outgoing)
                                        .await
                                        .unwrap();
                                }
                                None => {
                                    // TODO do we need to send ack if the message is mandatory or
                                    // immediate?
                                    if let Some(dt) = self.next_confirm_delivery_tag.get_mut(&channel) {
                                        // We can keep this mut shorter, if it matters
                                        self.outgoing
                                            .send(Frame::Frame(
                                                frame::BasicAckArgs::default().delivery_tag(*dt).frame(channel),
                                            ))
                                            .await
                                            .unwrap();

                                        *dt += 1;
                                    }
                                }
                            }
                        }
                    }
                    None => {
                        // FIXME this is not good, if we haven't declare the exchange it won't
                        // exists from the point of view of this code. But this is not correct.
                        // If we don't have the exchange it means that we haven't sent anything to
                        // that exchange yet.
                        if msg.mandatory {
                            logerr!(message::send_basic_return(Arc::new(msg), self.frame_max, &self.outgoing).await);
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
