//! Messages are sent to exchanges and forwarded to queues. There is a
//! possibility to state that a message is processed via an oneshot channel.
use crate::client::ChannelError;
use crate::queue::handler::FrameSink;
use crate::queue::handler::Tag;
use crate::{chk, send, Result};
use metalmq_codec::codec::Frame;
use metalmq_codec::frame::{self, AMQPFrame};
use std::fmt;
use std::sync::Arc;

#[derive(Clone, Default)]
pub struct Message {
    /// Id of the connection sent this message.
    pub source_connection: String,
    pub channel: frame::Channel,
    pub exchange: String,
    pub routing_key: String,
    /// If message is mandatory but there is no queue bound to the exchange, the server sends the
    /// undeliverable message with a basic return method.
    pub mandatory: bool,
    /// If message is immediate and it can be sent to a queue but there is no consumer on the
    /// queue, the server returns an undeliverable message with a basic return method.
    pub immediate: bool,
    pub content: MessageContent,
}

#[derive(Clone, Default)]
pub struct MessageContent {
    pub class_id: frame::ClassId,
    pub weight: frame::Weight,
    pub body: Vec<u8>,
    pub body_size: u64,
    pub prop_flags: frame::HeaderPropertyFlags,
    pub cluster_id: Option<String>,
    pub app_id: Option<String>,
    pub user_id: Option<String>,
    pub message_type: Option<String>,
    pub timestamp: Option<u64>,
    pub message_id: Option<String>,
    pub expiration: Option<String>,
    pub reply_to: Option<String>,
    pub correlation_id: Option<String>,
    pub priority: Option<u8>,
    pub delivery_mode: Option<u8>,
    pub headers: Option<frame::FieldTable>,
    pub content_encoding: Option<String>,
    pub content_type: Option<String>,
}

impl fmt::Debug for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let body_len = std::cmp::min(64usize, self.content.body.len());
        let body = String::from_utf8_lossy(&self.content.body[..body_len]);

        f.debug_struct("Message")
            .field("connection", &self.source_connection)
            .field("channel", &self.channel)
            .field("body", &body.to_string())
            .finish()
    }
}

/// Create content header and content body frames from a message. If content is bigger than the
/// frame size, it makes more content body frames.
pub fn message_to_content_frames(
    channel: frame::Channel,
    content: MessageContent,
    frame_size: usize,
) -> Vec<frame::AMQPFrame> {
    let header = frame::ContentHeaderFrame {
        channel,
        class_id: content.class_id, // TODO ???
        weight: content.weight,
        body_size: content.body_size,
        prop_flags: content.prop_flags,
        cluster_id: content.cluster_id,
        app_id: content.app_id,
        user_id: content.user_id,
        message_type: content.message_type,
        timestamp: content.timestamp,
        message_id: content.message_id,
        expiration: content.expiration,
        reply_to: content.reply_to,
        correlation_id: content.correlation_id,
        priority: content.priority,
        delivery_mode: content.delivery_mode,
        headers: content.headers,
        content_encoding: content.content_encoding,
        content_type: content.content_type,
    };

    let mut frames = vec![AMQPFrame::ContentHeader(header)];

    for chunk in content.body.chunks(frame_size) {
        frames.push(AMQPFrame::ContentBody(frame::content_body(channel, chunk)));
    }

    frames
}

/// Send out a message to the specified channel.
pub async fn send_message(
    channel: frame::Channel,
    message: Arc<Message>,
    tag: &Tag,
    frame_size: usize,
    outgoing: &FrameSink,
) -> Result<()> {
    let mut frames = message_to_content_frames(channel, message.content.clone(), frame_size);

    let basic_deliver = frame::basic_deliver(
        channel,
        &tag.consumer_tag,
        tag.delivery_tag,
        false,
        &message.exchange,
        &message.routing_key,
    );
    frames.insert(0, basic_deliver);

    chk!(send!(outgoing, Frame::Frames(frames)))?;

    Ok(())
}

pub async fn send_basic_return(message: Arc<Message>, frame_size: usize, outgoing: &FrameSink) -> Result<()> {
    let mut frames = message_to_content_frames(message.channel, message.content.clone(), frame_size);

    frames.insert(
        0,
        frame::basic_return(
            message.channel,
            ChannelError::NoRoute as u16,
            "NO_ROUTE",
            &message.exchange,
            &message.routing_key,
        ),
    );

    // FIXME why the channel here is fixed?
    frames.push(frame::basic_ack(message.channel, 1u64, false));

    chk!(send!(outgoing, Frame::Frames(frames)))?;

    Ok(())
}

pub async fn send_basic_get_ok(
    channel: u16,
    delivery_tag: u64,
    message: Arc<Message>,
    message_count: u32,
    frame_size: usize,
    outgoing: &FrameSink,
) -> Result<()> {
    let mut frames = message_to_content_frames(channel, message.content.clone(), frame_size);
    let flags = frame::BasicGetOkFlags::default();
    // TODO handle redelivered

    let basic_get = frame::basic_get_ok(
        channel,
        delivery_tag,
        Some(flags),
        &message.exchange,
        &message.routing_key,
        message_count,
    );
    frames.insert(0, basic_get);

    chk!(send!(outgoing, Frame::Frames(frames)))?;

    Ok(())
}

#[cfg(test)]
pub mod tests {
    use super::*;

    pub fn empty_message() -> Message {
        Message {
            channel: 1,
            source_connection: "".to_owned(),
            exchange: "".to_owned(),
            routing_key: "".to_owned(),
            content: MessageContent {
                body: b"".to_vec(),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn check_zero_length_body() {
        let message = empty_message();

        let frames = message_to_content_frames(1, message.content, 32_768);

        assert_eq!(1, frames.len());
    }
}
