use std::collections::HashMap;

use metalmq_codec::frame::{
    fieldtable_to_hashmap, AMQPFieldValue, ContentBodyFrame, ContentHeaderFrame, HeaderPropertyFlags,
};

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
    /// Mime content type
    pub content_type: Option<String>,
    /// Mime content encoding
    pub content_encoding: Option<String>,
    pub headers: HashMap<String, String>,
    /// 1 - non-persistent, 2 - persistent
    pub delivery_mode: Option<u8>,
    /// Priority between 0..9
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
        MessageProperties {
            content_type: value.content_type,
            content_encoding: value.content_encoding,
            headers: fieldtable_to_hashmap(value.headers.unwrap()),
            delivery_mode: value.delivery_mode,
            priority: value.priority,
            correlation_id: value.correlation_id,
            reply_to: value.reply_to,
            expiration: value.expiration,
            message_id: value.message_id,
            timestamp: value.timestamp,
            message_type: value.message_type,
            user_id: value.user_id,
            app_id: value.app_id,
        }
    }
}

impl From<&str> for PublishedMessage {
    fn from(value: &str) -> Self {
        Self {
            message: Content {
                channel: 0u16,
                body: value.as_bytes().to_vec(),
                properties: MessageProperties {
                    content_type: Some("text/plain".into()),
                    content_encoding: Some("UTF-8".into()),
                    ..Default::default()
                },
            },
            ..Default::default()
        }
    }
}

impl PublishedMessage {
    pub fn channel(mut self, value: ChannelNumber) -> Self {
        self.message.channel = value;
        self
    }

    pub fn text<T>(mut self, value: T) -> Self
    where
        T: Into<Vec<u8>>,
    {
        self.message.body = value.into();
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
