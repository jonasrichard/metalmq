use crate::{
    error::{connection_error, ConnectionError, Result},
    message::{Message, MessageContent},
};
use metalmq_codec::frame;

use super::types::PublishedContent;

impl PublishedContent {
    pub fn handle_content_header(&mut self, header: frame::ContentHeaderFrame) -> Result<()> {
        // Class Id in content header must match to the class id of the method frame initiated
        // the sending of the content.
        if header.class_id != (self.method_frame_class_id >> 16) as u16 {
            return connection_error(
                self.method_frame_class_id,
                ConnectionError::FrameError,
                "Class ID in content header must match that of the method frame",
            );
        }

        // Weight must be zero.
        if header.weight != 0 {
            return connection_error(
                self.method_frame_class_id,
                ConnectionError::FrameError,
                "Weight must be 0",
            );
        }

        if header.channel == 0 {
            return connection_error(
                self.method_frame_class_id,
                ConnectionError::ChannelError,
                "Channel must not be 0",
            );
        }

        self.content_header = header;

        Ok(())
    }

    pub fn handle_content_body(&mut self, body: frame::ContentBodyFrame) -> Result<()> {
        self.body_size += body.body.len();
        self.content_bodies.push(body);

        Ok(())
    }

    // Send the message to an exchange or directly to the queue if the destination is the default
    // exchange (exchange with name `""`).
    //pub async fn send_message(message: Message, exchange_channel: &Sender<ExchangeCommand>) -> Result<()> {
    //    // If message is mandatory or the channel is in confirm mode we can expect
    //    // returned message.
    //    let (tx, rx) = match message.mandatory || self.next_confirm_delivery_tag.contains_key(&message.channel) {
    //        false => (None, None),
    //        true => {
    //            let (tx, rx) = oneshot::channel();
    //            (Some(tx), Some(rx))
    //        }
    //    };

    //    let cmd = ExchangeCommand::Message { message, returned: tx };

    //    // TODO is this the correct way of returning Err(_)
    //    logerr!(exchange_channel.send_timeout(cmd, Duration::from_secs(1)).await);

    //    Ok(())
    //}
}

impl From<PublishedContent> for Message {
    fn from(value: PublishedContent) -> Self {
        let mut message_body = vec![];

        // TODO we shouldn't concatenate the body parts, because we need to send them in
        // chunks anyway. Can a consumer support less or more frame size than a server?
        for cb in value.content_bodies {
            message_body.extend(cb.body);
        }

        Message {
            source_connection: value.source_connection,
            channel: value.channel,
            exchange: value.exchange,
            routing_key: value.routing_key,
            mandatory: value.mandatory,
            immediate: value.immediate,
            content: MessageContent {
                class_id: value.content_header.class_id,
                weight: value.content_header.weight,
                body: message_body,
                body_size: value.content_header.body_size,
                prop_flags: value.content_header.prop_flags,
                content_encoding: value.content_header.content_encoding,
                content_type: value.content_header.content_type,
                delivery_mode: value.content_header.delivery_mode,
                message_id: value.content_header.message_id,
                timestamp: value.content_header.timestamp,
                headers: value.content_header.headers,
                // TODO copy all the message properties
                ..Default::default()
            },
        }
    }
}
