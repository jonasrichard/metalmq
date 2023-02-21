mod confirm;
mod direct;
mod exchange;
mod fanout;
mod get;
mod helper;
mod queue;
mod topic;

use metalmq_client::*;

pub fn unwrap_delivered_message(signal: ConsumerSignal) -> DeliveredMessage {
    match signal {
        ConsumerSignal::Delivered(msg) => *msg,
        other => panic!("{other:?} is not a Deliver signal"),
    }
}

pub fn message_from_string(channel: ChannelNumber, body: &str) -> PublishedMessage {
    use std::time::{SystemTime, UNIX_EPOCH};

    PublishedMessage {
        message: Content {
            channel,
            body: body.as_bytes().to_vec(),
            properties: MessageProperties {
                content_type: Some("plain/text".into()),
                content_encoding: Some("UTF-8".into()),
                message_id: Some(uuid::Uuid::new_v4().as_hyphenated().to_string()),
                timestamp: Some(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()),
                user_id: Some("integration-test".into()),
                app_id: Some("metalmq-client-test".into()),
                ..Default::default()
            },
        },
        ..Default::default()
    }
}
