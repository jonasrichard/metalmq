use std::time::Duration;

use log::{error, warn};
use metalmq_codec::{codec::Frame, frame};
use tokio::sync::oneshot;

use crate::{
    client::channel::types::{ActivelyConsumedQueue, Channel, PassivelyConsumedQueue, PublishedContent},
    error::{ChannelError, ConnectionError, Result},
    exchange::{self, handler::ExchangeCommand, manager::GetExchangeSinkQuery},
    message::{self, Message},
    queue,
};

impl Channel {
    pub async fn handle_basic_publish(&mut self, args: frame::BasicPublishArgs) -> Result<()> {
        if self.in_flight_content.is_some() {
            return ConnectionError::UnexpectedFrame.to_result(frame::BASIC_PUBLISH, "Already publish message arrived");
        }

        // Check if exchange exists, and cache it in order that `handle_content_body` can access
        // that.
        if !self.exchanges.contains_key(&args.exchange_name) {
            let cmd = GetExchangeSinkQuery {
                exchange_name: args.exchange_name.clone(),
            };

            if let Some(ex_tx) = exchange::manager::get_exchange_sink(&self.em, cmd).await {
                self.exchanges.insert(args.exchange_name.clone(), ex_tx);
            } else {
                return ChannelError::NotFound.to_result(self.number, frame::BASIC_PUBLISH, "Exchange not exist");
            }
        }

        let flags = args.flags;

        let content = PublishedContent {
            source_connection: self.source_connection.clone(),
            channel: self.number,
            exchange: args.exchange_name,
            routing_key: args.routing_key,
            mandatory: flags.contains(frame::BasicPublishFlags::MANDATORY),
            immediate: flags.contains(frame::BasicPublishFlags::IMMEDIATE),
            method_frame_class_id: frame::BASIC_PUBLISH,
            ..Default::default()
        };

        self.in_flight_content = Some(content);

        Ok(())
    }

    pub async fn handle_basic_consume(&mut self, args: frame::BasicConsumeArgs) -> Result<()> {
        let queue_clone = args.queue.clone();
        let consumer_tag_clone = args.consumer_tag.clone();
        let cmd = queue::manager::QueueConsumeCommand {
            conn_id: self.source_connection.clone(),
            channel: self.number,
            queue_name: queue_clone,
            consumer_tag: consumer_tag_clone,
            no_ack: args.flags.contains(frame::BasicConsumeFlags::NO_ACK),
            exclusive: args.flags.contains(frame::BasicConsumeFlags::EXCLUSIVE),
            outgoing: self.outgoing.clone(),
            frame_size: self.frame_size,
        };

        let queue_sink = queue::manager::consume(&self.qm, cmd).await?;

        let consumer_tag_clone = args.consumer_tag.clone();
        let queue_sink_clone = queue_sink.clone();

        self.consumed_queue = Some(ActivelyConsumedQueue {
            consumer_tag: args.consumer_tag.clone(),
            queue_name: args.queue.clone(),
            queue_sink,
        });

        self.send_frame(Frame::Frame(
            frame::BasicConsumeOkArgs::new(&args.consumer_tag).frame(self.number),
        ))
        .await?;

        let start_deliver_cmd = queue::handler::QueueCommand::StartDelivering {
            consumer_tag: consumer_tag_clone,
        };

        queue_sink_clone.send(start_deliver_cmd).await?;

        Ok(())
    }

    pub async fn handle_basic_cancel(&mut self, args: frame::BasicCancelArgs) -> Result<()> {
        if let Some(cq) = self.consumed_queue.take() {
            let cmd = queue::manager::QueueCancelConsume {
                channel: self.number,
                queue_name: cq.queue_name.clone(),
                consumer_tag: cq.consumer_tag.clone(),
            };
            queue::manager::cancel_consume(&self.qm, cmd).await?;
        }

        self.send_frame(Frame::Frame(
            frame::BasicCancelOkArgs::new(&args.consumer_tag).frame(self.number),
        ))
        .await
    }

    /// Handles Ack coming from client.
    ///
    /// A message can be acked more than once. If a non-delivered message is acked, a channel
    /// exception will be raised.
    pub async fn handle_basic_ack(&mut self, args: frame::BasicAckArgs) -> Result<()> {
        // TODO check if only delivered messages are acked, even multiple times
        match &self.consumed_queue {
            Some(cq) => {
                let (tx, rx) = oneshot::channel();

                // TODO why we need here the timeout?
                cq.queue_sink
                    .send_timeout(
                        queue::handler::QueueCommand::AckMessage(queue::handler::AckCmd {
                            channel: self.number,
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
            None => match &self.passively_consumed_queue {
                Some(pq) => {
                    let (tx, rx) = oneshot::channel();

                    pq.queue_sink
                        .send(queue::handler::QueueCommand::AckMessage(queue::handler::AckCmd {
                            channel: self.number,
                            consumer_tag: pq.consumer_tag.clone(),
                            delivery_tag: args.delivery_tag,
                            multiple: args.multiple,
                            result: tx,
                        }))
                        .await?;

                    rx.await.unwrap().unwrap();
                }
                None => {
                    warn!("Basic.Ack arrived without consuming the queue");
                }
            },
        }

        Ok(())
    }

    pub async fn handle_basic_get(&mut self, args: frame::BasicGetArgs) -> Result<()> {
        // Cache the queue the client consumes passively with Basic.Get
        if self.passively_consumed_queue.is_none() {
            let sink = queue::manager::get_command_sink(
                &self.qm,
                queue::manager::GetQueueSinkQuery {
                    channel: self.number,
                    queue_name: args.queue.clone(),
                },
            )
            .await;

            if sink.is_err() {
                return ChannelError::NotFound.to_result(
                    self.number,
                    frame::BASIC_GET,
                    &format!("Queue {} not found", args.queue),
                );
            }

            let sink = sink.unwrap();
            let (tx, rx) = oneshot::channel();

            sink.send(queue::handler::QueueCommand::PassiveConsume(
                queue::handler::PassiveConsumeCmd {
                    conn_id: self.source_connection.clone(),
                    channel: self.number,
                    sink: self.outgoing.clone(),
                    frame_size: self.frame_size,
                    result: tx,
                },
            ))
            .await
            .unwrap();

            rx.await.unwrap().unwrap();

            let pq = PassivelyConsumedQueue {
                queue_name: args.queue,
                consumer_tag: format!("{}-{}", self.source_connection, self.number),
                delivery_tag: 1u64,
                queue_sink: sink,
            };

            let _ = self.passively_consumed_queue.insert(pq);
        }

        if let Some(pq) = &self.passively_consumed_queue {
            let (tx, rx) = oneshot::channel();

            let _ = pq
                .queue_sink
                .send(queue::handler::QueueCommand::Get(queue::handler::GetCmd {
                    conn_id: self.source_connection.clone(),
                    channel: self.number,
                    no_ack: args.no_ack,
                    result: tx,
                }))
                .await
                .unwrap();

            rx.await.unwrap()
        } else {
            ConnectionError::InternalError.to_result(frame::BASIC_GET, "Queue not exist")
        }
    }

    pub async fn handle_basic_reject(&mut self, __args: frame::BasicRejectArgs) -> Result<()> {
        Ok(())
    }

    pub async fn handle_confirm_select(&mut self, _args: frame::ConfirmSelectArgs) -> Result<()> {
        self.next_confirm_delivery_tag = Some(1u64);

        self.send_frame(Frame::Frame(frame::confirm_select_ok(self.number)))
            .await?;

        Ok(())
    }

    pub async fn handle_content_header(&mut self, header: frame::ContentHeaderFrame) -> Result<()> {
        if self.in_flight_content.is_none() {
            return ConnectionError::UnexpectedFrame.to_result(0, "Unexpected content header");
        }

        if let Some(content) = &mut self.in_flight_content {
            content.content_header = header;
        }

        Ok(())
    }

    pub async fn handle_content_body(&mut self, body: frame::ContentBodyFrame) -> Result<()> {
        let channel = body.channel;

        if body.body.len() > 131_072 {
            error!("Content is too large {}", body.body.len());

            return ChannelError::ContentTooLarge.to_result(channel, frame::BASIC_PUBLISH, "Body is too long");
        }

        if let Some(pc) = &mut self.in_flight_content {
            pc.handle_content_body(body)?;

            if pc.body_size < pc.content_header.body_size as usize {
                return Ok(());
            }
        }

        if let Some(pc) = self.in_flight_content.take() {
            let msg: Message = pc.into();

            if let Some(ex_tx) = self.exchanges.get(&msg.exchange) {
                // In confirm mode or if message is mandatory we need to be sure that the message
                // was router correctly from the exchange to the queue.
                let (rtx, rrx) = match msg.mandatory || self.next_confirm_delivery_tag.is_some() {
                    false => (None, None),
                    true => {
                        let (tx, rx) = oneshot::channel();

                        (Some(tx), Some(rx))
                    }
                };

                let cmd = ExchangeCommand::Message {
                    message: msg,
                    returned: rtx,
                };

                ex_tx.send(cmd).await.unwrap();

                if let Some(rx) = rrx {
                    match rx.await.unwrap() {
                        Some(returned_message) => {
                            message::send_basic_return(returned_message, self.frame_size, &self.outgoing)
                                .await
                                .unwrap();
                        }
                        None => {
                            // TODO do we need to send ack if the message is mandatory or
                            // immediate?
                            if let Some(dt) = &mut self.next_confirm_delivery_tag {
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
        }

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

        //match self.exchanges.get(&msg.exchange) {
        //    Some(ch) => {
        //        // If message is mandatory or the channel is in confirm mode we can expect
        //        // returned message.
        //        let (tx, rx) = match msg.mandatory || self.next_confirm_delivery_tag.contains_key(&channel) {
        //            false => (None, None),
        //            true => {
        //                let (tx, rx) = oneshot::channel();
        //                (Some(tx), Some(rx))
        //            }
        //        };

        //        let cmd = ExchangeCommand::Message {
        //            message: msg,
        //            returned: tx,
        //        };

        //        // TODO is this the correct way of returning Err(_)
        //        logerr!(ch.send_timeout(cmd, Duration::from_secs(1)).await);

        //        if let Some(rx) = rx {
        //            match rx.await.unwrap() {
        //                Some(returned_message) => {
        //                    message::send_basic_return(returned_message, self.frame_max, &self.outgoing)
        //                        .await
        //                        .unwrap();
        //                }
        //                None => {
        //                    // TODO do we need to send ack if the message is mandatory or
        //                    // immediate?
        //                    if let Some(dt) = self.next_confirm_delivery_tag.get_mut(&channel) {
        //                        // We can keep this mut shorter, if it matters
        //                        self.outgoing
        //                            .send(Frame::Frame(
        //                                frame::BasicAckArgs::default().delivery_tag(*dt).frame(channel),
        //                            ))
        //                            .await
        //                            .unwrap();

        //                        *dt += 1;
        //                    }
        //                }
        //            }
        //        }
        //    }
        //    None => {
        //        // FIXME this is not good, if we haven't declare the exchange it won't
        //        // exists from the point of view of this code. But this is not correct.
        //        // If we don't have the exchange it means that we haven't sent anything to
        //        // that exchange yet.
        //        if msg.mandatory {
        //            logerr!(message::send_basic_return(Arc::new(msg), self.frame_max, &self.outgoing).await);
        //        }
        //    }
        //}

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use frame::{BasicPublishArgs, BasicPublishFlags, ContentBodyFrame, ContentHeaderFrame};
    use tokio::sync::mpsc;

    use crate::exchange;
    use crate::tests::recv::recv_with_timeout;

    use super::*;

    #[tokio::test]
    async fn complete_published_content_should_be_sent_to_exchange() -> Result<()> {
        let (ex_tx, mut ex_rx) = mpsc::channel(16);
        let (tx, _rx) = mpsc::channel(16);

        let (em_tx, _em_rx) = mpsc::channel(16);
        let (qm_tx, _qm_rx) = mpsc::channel(16);

        let mut channel = Channel {
            source_connection: "12345-12345".into(),
            number: 1,
            consumed_queue: None,
            passively_consumed_queue: None,
            in_flight_content: None,
            confirm_mode: false,
            next_confirm_delivery_tag: None,
            frame_size: 65535,
            outgoing: tx,
            exchanges: HashMap::new(),
            em: em_tx,
            qm: qm_tx,
        };

        channel.exchanges.insert("x-target".into(), ex_tx);

        channel
            .handle_basic_publish(BasicPublishArgs {
                exchange_name: "x-target".into(),
                routing_key: "".into(),
                flags: BasicPublishFlags::default(),
            })
            .await?;

        channel
            .handle_content_header(ContentHeaderFrame {
                channel: 1,
                ..Default::default()
            })
            .await?;

        let body = ContentBodyFrame {
            channel: 1,
            body: b"Test message".to_vec(),
        };

        channel.handle_content_body(body).await?;

        let cmd = recv_with_timeout(&mut ex_rx).await.expect("No frame received");

        assert!(matches!(
            cmd,
            exchange::handler::ExchangeCommand::Message {
                message: _,
                returned: None
            }
        ));

        Ok(())
    }
}
