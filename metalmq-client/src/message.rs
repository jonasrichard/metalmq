use std::collections::HashMap;

use metalmq_codec::frame::{AMQPFieldValue, ContentBodyFrame, ContentHeaderFrame, HeaderPropertyFlags};

use crate::ChannelNumber;

/// A message sent to the server or received from the server.
#[derive(Debug, Default)]
pub struct Content {
    pub channel: ChannelNumber,
    pub body: Vec<u8>,
    pub properties: MessageProperties,
}

/// Standard set of message properties
#[derive(Debug, Default)]
pub struct MessageProperties {
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub headers: HashMap<String, String>,
    /// 1 - non-persistent, 2 - persistent
    pub delivery_mode: Option<u8>,
    pub priority: Option<u8>,
    pub correlation_id: Option<String>,
    pub reply_to: Option<String>,
    pub expiration: Option<String>,
    pub message_id: Option<String>,
    pub timestamp: Option<u64>,
    pub message_type: Option<String>,
    pub user_id: Option<String>,
    pub app_id: Option<String>,
}

/// A delivered message.
///
/// With the `consumer_tag` and `delivery_tag` a client can send back acknowledgements to the
/// server, saying that the message was successfully arrived.
#[derive(Debug, Default)]
pub struct DeliveredMessage {
    pub message: Content,
    pub consumer_tag: String,
    pub delivery_tag: u64,
    pub redelivered: bool,
    pub exchange: String,
    pub routing_key: String,
}

/// A message get by `Basic.Get`.
#[derive(Debug, Default)]
pub struct GetMessage {
    pub message: Content,
    pub delivery_tag: u64,
    pub redelivered: bool,
    pub exchange: String,
    pub routing_key: String,
    pub message_count: u32,
}

/// A message returned to the client.
#[derive(Debug, Default)]
pub struct ReturnedMessage {
    pub message: Content,
    // TODO use enums here
    pub reply_code: u16,
    pub reply_text: String,
    pub exchange: String,
    pub routing_key: String,
}

/// A message published by the client.
#[derive(Debug, Default)]
pub struct PublishedMessage {
    pub message: Content,
    pub mandatory: bool,
    pub immediate: bool,
}

/// Internally it is comfortable to handle delivered or returned message in the same variable.
#[derive(Debug)]
pub(crate) enum Message {
    Delivered(DeliveredMessage),
    Get(GetMessage),
    Returned(ReturnedMessage),
}

pub(crate) fn to_content_frames(message: Content) -> (ContentHeaderFrame, ContentBodyFrame) {
    let mut headers = HashMap::new();

    for (k, v) in message.properties.headers {
        headers.insert(k, AMQPFieldValue::LongString(v));
    }

    // FIXME set header property flags according to the values
    let header = ContentHeaderFrame {
        channel: message.channel,
        class_id: 0,
        weight: 0,
        body_size: message.body.len() as u64,
        prop_flags: HeaderPropertyFlags::default(),
        cluster_id: None,
        app_id: message.properties.app_id,
        user_id: message.properties.user_id,
        message_type: message.properties.message_type,
        timestamp: message.properties.timestamp,
        message_id: message.properties.message_id,
        expiration: message.properties.expiration,
        reply_to: message.properties.reply_to,
        correlation_id: message.properties.correlation_id,
        priority: message.properties.priority,
        delivery_mode: message.properties.delivery_mode,
        headers: Some(headers),
        content_encoding: message.properties.content_encoding,
        content_type: message.properties.content_type,
    };

    let body = ContentBodyFrame {
        channel: message.channel,
        body: message.body,
    };

    (header, body)
}

impl From<ContentHeaderFrame> for MessageProperties {
    fn from(value: ContentHeaderFrame) -> Self {
        MessageProperties::default()
    }
}

impl From<&str> for PublishedMessage {
    fn from(value: &str) -> Self {
        Self {
            message: Content {
                channel: 0u16,
                body: value.as_bytes().to_vec(),
                properties: MessageProperties::default(),
            },
            ..Default::default()
        }
    }
}

impl PublishedMessage {
    pub fn str(mut self, value: &str) -> Self {
        self.message.body = value.as_bytes().to_vec();
        self
    }

    /// Condition for mandatory publishing. Mandatory messages are failed if the exchange doesn't have
    /// bound queue or if the routing keys are not matched.
    pub fn mandatory(mut self, value: bool) -> Self {
        self.mandatory = value;
        self
    }

    /// Condition for immediate publishing. Immediate messages are received by a server successfully if
    /// they managed to be sent to a consumer immediately.
    pub fn immediate(mut self, value: bool) -> Self {
        self.immediate = value;
        self
    }
}
