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
        ConsumerSignal::Delivered(msg) => msg,
        other => panic!("{other:?} is not a Deliver signal"),
    }
}

pub fn message_from_string(channel: ChannelNumber, body: String) -> PublishedMessage {
    PublishedMessage {
        message: Content {
            channel,
            body: body.as_bytes().to_vec(),
            ..Default::default()
        },
        ..Default::default()
    }
}
