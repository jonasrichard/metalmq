use crate::Result;
use ironmq_codec::frame;
use ironmq_codec::frame::{MethodFrame};
use log::info;

#[derive(Debug)]
pub(crate) struct ClientState {
}

#[derive(Debug)]
pub(crate) enum Command {
    ConnectionInit,
    ConnectionStartOk(Box<ConnStartOkArgs>),
    ConnectionTuneOk(Box<ConnTuneOkArgs>),
    ConnectionOpen(Box<ConnOpenArgs>),
    ConnectionClose,
    ChannelOpen(Box<ChannelOpenArgs>),
    ExchangeDeclare(Box<ExchangeDeclareArgs>),
    QueueDeclare(Box<QueueDeclareArgs>),
    QueueBind(Box<QueueBindArgs>)
}

#[derive(Debug)]
pub(crate) struct ConnStartArgs {
    pub(crate) channel: u16,
    pub(crate) properties: Vec<(String, frame::AMQPValue)>
}

#[derive(Debug)]
pub(crate) struct ConnStartOkArgs {
    pub(crate) channel: u16
}

impl From<MethodFrame> for ConnStartArgs {
    fn from(mf: MethodFrame) -> ConnStartArgs {
        ConnStartArgs {
            channel: mf.channel,
            properties: vec![]
        }
    }
}

impl From<ConnStartOkArgs> for MethodFrame {
    fn from(c: ConnStartOkArgs) -> MethodFrame {
        MethodFrame {
            channel: c.channel,
            class_method: frame::CONNECTION_START_OK,
            args: vec![]
        }
    }
}
pub(crate) fn connection_start(cs: &mut ClientState, properties: ConnStartArgs) -> Result<ConnStartOkArgs> {
    Ok(ConnStartOkArgs {
        channel: 0
    })
}

pub(crate) fn connection_start_ok(cs: &mut ClientState, args: ConnStartOkArgs) -> Result<()> {
    // TODO store start-ok properties in the client state
    Ok(())
}

#[derive(Debug)]
pub(crate) struct ConnTuneArgs {
}

#[derive(Debug)]
pub(crate) struct ConnTuneOkArgs {
}

impl From<MethodFrame> for ConnTuneArgs {
    fn from(fr: MethodFrame) -> ConnTuneArgs {
        ConnTuneArgs {
        }
    }
}

impl From<ConnTuneOkArgs> for MethodFrame {
    fn from(tp: ConnTuneOkArgs) -> MethodFrame {
        MethodFrame {
            channel: 0,
            class_method: frame::CONNECTION_TUNE_OK,
            args: vec![]
        }
    }
}

pub(crate) fn connection_tune(cs: &mut ClientState, tune_properties: ConnTuneArgs) -> Result<ConnTuneOkArgs> {
    Ok(ConnTuneOkArgs {
    })
}

pub(crate) fn connection_tune_ok(cs: &mut ClientState, args: ConnTuneOkArgs) -> Result<()> {
    Ok(())
}

#[derive(Debug)]
pub(crate) struct ConnOpenArgs {
    pub(crate) virtual_host: String,
    pub(crate) insist: bool
}

#[derive(Debug)]
pub(crate) struct ConnOpenOkArgs {
    pub(crate) known_host: String
}

impl From<ConnOpenOkArgs> for MethodFrame {
    fn from(args: ConnOpenOkArgs) -> MethodFrame {
        frame::connection_open_ok(0)
    }
}

pub(crate) fn connection_open(cs: &mut ClientState, args: ConnOpenArgs) -> Result<ConnOpenOkArgs> {
    Ok(ConnOpenOkArgs {
        known_host: "".into()
    })
}

#[derive(Debug)]
pub(crate) struct ChannelOpenArgs {
    pub(crate) channel: u16
}

#[derive(Debug)]
pub(crate) struct ChannelOpenOkArgs {
    pub(crate) channel: u16
}

impl From<ChannelOpenArgs> for MethodFrame {
    fn from(args: ChannelOpenArgs) -> MethodFrame {
        MethodFrame {
            channel: args.channel,
            class_method:frame::CHANNEL_OPEN,
            args: vec![]
        }
    }
}

impl From<MethodFrame> for ChannelOpenOkArgs {
    fn from(frame: MethodFrame) -> ChannelOpenOkArgs {
        ChannelOpenOkArgs {
            channel: frame.channel
        }
    }
}

#[derive(Debug)]
pub(crate) struct ExchangeDeclareArgs {
    pub(crate) channel: u16,
    pub(crate) exchange_name: String,
    pub(crate) exchange_type: String
}

#[derive(Debug)]
pub(crate) struct ExchangeDeclareOkArgs {
    pub(crate) channel: u16
}

impl From<ExchangeDeclareOkArgs> for MethodFrame {
    fn from(args: ExchangeDeclareOkArgs) -> MethodFrame {
        frame::exchange_declare_ok(args.channel)
    }
}

impl From<MethodFrame> for ExchangeDeclareArgs {
    fn from(mf: MethodFrame) -> ExchangeDeclareArgs {
        ExchangeDeclareArgs {
            channel: mf.channel,
            exchange_name: "xchg-name".into(),
            exchange_type: "fanout".into()
        }
    }
}

pub(crate) fn exchange_declare(cs: &mut ClientState, args: ExchangeDeclareArgs) -> Result<ExchangeDeclareOkArgs> {
    Ok(ExchangeDeclareOkArgs {
        channel: args.channel
    })
}

#[derive(Debug)]
pub(crate) struct QueueDeclareArgs {
    pub(crate) channel: u16,
    pub(crate) queue_name: String
}

#[derive(Debug)]
pub(crate) struct QueueDeclareOkArgs {
    pub(crate) channel: u16
}

impl From<MethodFrame> for QueueDeclareArgs {
    fn from(mf: MethodFrame) -> QueueDeclareArgs {
        QueueDeclareArgs {
            channel: mf.channel,
            queue_name: "queue".into()
        }
    }
}

impl From<QueueDeclareOkArgs> for MethodFrame {
    fn from(args: QueueDeclareOkArgs) -> MethodFrame {
        frame::queue_declare_ok(args.channel, "queue".into(), 0, 0)
    }
}

pub(crate) fn queue_declare(cs: &mut ClientState, args: QueueDeclareArgs) -> Result<QueueDeclareOkArgs> {
    Ok(QueueDeclareOkArgs {
        channel: args.channel
    })
}

#[derive(Debug)]
pub(crate) struct QueueBindArgs {
    pub(crate) channel: u16,
    pub(crate) queue_name: String,
    pub(crate) exchange_name: String,
    pub(crate) routing_key: String
}

#[derive(Debug)]
pub(crate) struct QueueBindOkArgs {
    pub(crate) channel: u16
}

impl From<MethodFrame> for QueueBindArgs {
    fn from(mf: MethodFrame) -> QueueBindArgs {
        QueueBindArgs {
            channel: mf.channel,
            queue_name: "queue".into(),
            exchange_name: "xchg-name".into(),
            routing_key: "".into()
        }
    }
}

impl From<QueueBindOkArgs> for MethodFrame {
    fn from(args: QueueBindOkArgs) -> MethodFrame {
        frame::queue_bind_ok(args.channel)
    }
}

pub(crate) fn queue_bind(cs: &mut ClientState, args: QueueBindArgs) -> Result<QueueBindOkArgs> {
    Ok(QueueBindOkArgs {
        channel: args.channel
    })
}

//???
enum Phase {
    Uninitialized,
    Connected,
    Authenticated,
    ChannelOpened,
    ChannelClosed,
    Closing
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn connect() {
    }
}
