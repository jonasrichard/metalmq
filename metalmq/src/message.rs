//! Messages are sent to exhchanges and forwarded to queues. There is a
//! possibility to state that a message is processed via an oneshot channel.
use crate::queue::handler::FrameSink;
use crate::queue::handler::Tag;
use crate::{chk, send, Result};
use metalmq_codec::codec::Frame;
use metalmq_codec::frame;
use std::fmt;
use tokio::sync::mpsc;

//pub(crate) type MessageId = String;

#[derive(Clone)]
pub(crate) struct Message {
    /// Id of the connection sent this message.
    pub(crate) source_connection: String,
    pub(crate) channel: u16, // TODO use channel type here
    pub(crate) content: Vec<u8>,
    pub(crate) exchange: String,
    pub(crate) routing_key: String,
    pub(crate) mandatory: bool,
    pub(crate) immediate: bool,
}

impl fmt::Debug for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let body = String::from_utf8_lossy(&self.content[..std::cmp::min(64usize, self.content.len())]);

        f.debug_struct("Message")
            .field("connection", &self.source_connection)
            .field("channel", &self.channel)
            .field("body", &body.to_string())
            .finish()
    }
}

pub(crate) type MessageSink = mpsc::Sender<Message>;

/// Create content header and content body frames from a message
pub(crate) fn message_to_content_frames(message: &Message) -> Vec<frame::AMQPFrame> {
    vec![
        frame::AMQPFrame::ContentHeader(frame::content_header(message.channel, message.content.len() as u64)),
        frame::AMQPFrame::ContentBody(frame::content_body(message.channel, message.content.as_slice())),
    ]
}

pub(crate) async fn send_message(message: &Message, tag: &Tag, outgoing: &FrameSink) -> Result<()> {
    let mut frames = message_to_content_frames(&message);

    let basic_deliver = frame::basic_deliver(
        message.channel,
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
