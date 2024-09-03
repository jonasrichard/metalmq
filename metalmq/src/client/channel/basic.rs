use log::error;
use metalmq_codec::frame;

use crate::{
    error::{ChannelError, ConnectionError, Result},
    exchange::handler::ExchangeCommand,
    message::Message,
};

use super::types::{Channel, PublishedContent};

impl Channel {
    pub async fn handle_basic_publish(&mut self, args: frame::BasicPublishArgs) -> Result<()> {
        //if let Some(exchange) = self.exchanges.get(&args.exchange_name) {
        //let cmd = ExchangeCommand::Message { message: todo!(), returned: todo!() };

        //exchange.send(cmd).await.unwrap();

        if self.in_flight_content.is_some() {
            return ConnectionError::UnexpectedFrame.to_result(frame::BASIC_PUBLISH, "Already publish message arrived");
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
        Ok(())
    }

    pub async fn handle_basic_cancel(&mut self, args: frame::BasicCancelArgs) -> Result<()> {
        Ok(())
    }

    /// Handles Ack coming from client.
    ///
    /// A message can be acked more than once. If a non-delivered message is acked, a channel
    /// exception will be raised.
    pub async fn handle_basic_ack(&mut self, args: frame::BasicAckArgs) -> Result<()> {
        Ok(())
    }

    pub async fn handle_basic_get(&mut self, args: frame::BasicGetArgs) -> Result<()> {
        Ok(())
    }

    pub async fn handle_basic_reject(&mut self, __args: frame::BasicRejectArgs) -> Result<()> {
        Ok(())
    }

    pub async fn handle_confirm_select(&mut self, _args: frame::ConfirmSelectArgs) -> Result<()> {
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
                let cmd = ExchangeCommand::Message {
                    message: msg,
                    returned: None,
                };

                ex_tx.send(cmd).await.unwrap();
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
