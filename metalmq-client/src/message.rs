use std::collections::HashMap;

use metalmq_codec::frame::{AMQPFieldValue, ContentBodyFrame, ContentHeaderFrame, HeaderPropertyFlags};

use crate::ChannelNumber;

/// A message received from the server.
///
/// With the `consumer_tag` and `delivery_tag` a client can send back acknowledgements to the
/// server, saying that the message was successfully arrived.
#[derive(Debug, Default)]
pub struct Message {
    pub channel: ChannelNumber,
    pub body: Vec<u8>,
    pub properties: MessageProperties,
    pub delivery_info: Option<DeliveryInfo>,
}

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

#[derive(Debug, Default)]
pub struct DeliveryInfo {
    pub consumer_tag: String,
    pub delivery_tag: u64,
    pub routing_key: String,
}

pub(crate) fn to_content_frames(message: Message) -> (ContentHeaderFrame, ContentBodyFrame) {
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
