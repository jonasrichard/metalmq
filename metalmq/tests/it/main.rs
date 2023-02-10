mod connection;
mod consume;
mod exchange;
mod helper;
mod publish;
mod queue_auto_delete;

use metalmq_client::*;

pub fn message_from_string(channel: ChannelNumber, body: String) -> Message {
    Message {
        channel,
        body: body.as_bytes().to_vec(),
        properties: MessageProperties::default(),
        delivery_info: None,
    }
}
