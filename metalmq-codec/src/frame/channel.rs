use super::{AMQPFrame, Channel, MethodFrameArgs};

#[derive(Debug, Default)]
pub struct ChannelCloseArgs {
    pub code: u16,
    pub text: String,
    pub class_id: u16,
    pub method_id: u16,
}

impl ChannelCloseArgs {
    pub fn frame(self, channel: Channel) -> AMQPFrame {
        AMQPFrame::Method(channel, super::CHANNEL_CLOSE, MethodFrameArgs::ChannelClose(self))
    }
}

pub fn channel_open(channel: Channel) -> AMQPFrame {
    AMQPFrame::Method(channel, super::CHANNEL_OPEN, MethodFrameArgs::ChannelOpen)
}

pub fn channel_open_ok(channel: Channel) -> AMQPFrame {
    AMQPFrame::Method(channel, super::CHANNEL_OPEN_OK, MethodFrameArgs::ChannelOpenOk)
}

pub fn channel_close(channel: Channel, code: u16, text: &str, class_method: u32) -> AMQPFrame {
    let (class_id, method_id) = super::split_class_method(class_method);

    AMQPFrame::Method(
        channel,
        super::CHANNEL_CLOSE,
        MethodFrameArgs::ChannelClose(ChannelCloseArgs {
            code,
            text: text.to_string(),
            class_id,
            method_id,
        }),
    )
}

pub fn channel_close_ok(channel: Channel) -> AMQPFrame {
    AMQPFrame::Method(channel, super::CHANNEL_CLOSE_OK, MethodFrameArgs::ChannelCloseOk)
}
