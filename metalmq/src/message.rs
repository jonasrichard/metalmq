//! Messages are sent to exchanges and forwarded to queues. There is a
//! possibility to state that a message is processed via an oneshot channel.
use crate::client::conn::SendFrame;
use crate::client::ChannelError;
use crate::queue::handler::FrameSink;
use crate::queue::handler::Tag;
use crate::{chk, send, Result};
use metalmq_codec::codec::Frame;
use metalmq_codec::frame::{self, AMQPFrame};
use std::fmt;

//pub type MessageId = String;

#[derive(Clone)]
pub struct Message {
    /// Id of the connection sent this message.
    pub source_connection: String,
    pub channel: u16, // TODO use channel type here
    pub content: Vec<u8>,
    pub exchange: String,
    pub routing_key: String,
    pub mandatory: bool,
    pub immediate: bool,
    // TODO add here all the necessary content header properties
    pub content_type: Option<String>,
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

//pub type MessageSink = mpsc::Sender<Message>;

/// Create content header and content body frames from a message
pub fn message_to_content_frames(message: &Message) -> Vec<frame::AMQPFrame> {
    let mut ch = frame::content_header(message.channel, message.content.len() as u64);
    message.content_type.as_ref().map(|v| ch.with_content_type(v.clone()));

    vec![
        AMQPFrame::ContentHeader(ch),
        AMQPFrame::ContentBody(frame::content_body(message.channel, message.content.as_slice())),
    ]
}

pub async fn send_message(message: &Message, tag: &Tag, outgoing: &FrameSink) -> Result<()> {
    let mut frames = message_to_content_frames(message);

    let basic_deliver = frame::basic_deliver(
        message.channel,
        &tag.consumer_tag,
        tag.delivery_tag,
        false,
        &message.exchange,
        &message.routing_key,
    );
    frames.insert(0, basic_deliver);

    chk!(send!(outgoing, SendFrame::Async(Frame::Frames(frames))))?;

    Ok(())
}

pub async fn send_basic_return(message: &Message, outgoing: &FrameSink) -> Result<()> {
    let mut frames = message_to_content_frames(message);

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

    frames.push(frame::basic_ack(message.channel, 1u64, false));

    chk!(send!(outgoing, SendFrame::Async(Frame::Frames(frames))))?;

    Ok(())
}

async fn x() -> Result<()> {
    use std::sync::Arc;
    use tokio::sync::mpsc;

    let data = Arc::new(Message {
        channel: 1u16,
        content: "Hello".to_string().as_bytes().to_vec(),
        exchange: "test".to_string(),
        immediate: false,
        mandatory: false,
        routing_key: "*".to_string(),
        source_connection: "1".to_string(),
        content_type: None,
    });

    let (tx, mut rx) = mpsc::channel::<Arc<Message>>(1);

    tokio::spawn(async move {
        if let Some(m) = rx.recv().await {
            let _ = m.channel;
        }
    });

    tx.send(data).await?;

    Ok(())
}

#[cfg(test)]
pub mod tests {
    use super::Message;

    pub fn empty_message() -> Message {
        Message {
            channel: 1,
            source_connection: "".to_owned(),
            content: b"".to_vec(),
            exchange: "".to_owned(),
            immediate: false,
            mandatory: false,
            routing_key: "".to_owned(),
            content_type: None,
        }
    }
}
