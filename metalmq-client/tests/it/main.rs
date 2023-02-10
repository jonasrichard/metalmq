mod direct;
mod exchange;
mod fanout;
mod helper;
mod queue;
mod topic;

use metalmq_client::*;

pub fn unwrap_delivered_message(signal: ConsumerSignal) -> Message {
    match signal {
        ConsumerSignal::Delivered(msg) => msg,
        other => panic!("{other:?} is not a Deliver signal"),
    }
}

pub fn message_from_string(channel: ChannelNumber, body: String) -> Message {
    Message {
        channel,
        body: body.as_bytes().to_vec(),
        properties: MessageProperties::default(),
        delivery_info: None,
    }
}
