use crate::Result;
use ironmq_codec::codec::{AMQPFrame, AMQPValue};
use log::info;

pub(crate) struct ClientState {
}

pub(crate) struct ServerProperties {
    channel: u16,
    properties: Vec<(String, AMQPValue)>
}

pub(crate) struct ClientProperties {
}

pub(crate) struct TuneProperties {
}

pub(crate) fn connection_start(cs: &mut ClientState, properties: ServerProperties) -> Result<ClientProperties> {
    Ok(ClientProperties {
    })
}

pub(crate) fn connection_tune(cs: &mut ClientState, tune_properties: TuneProperties) -> Result<TuneProperties> {
    Ok(TuneProperties {
    })
}

impl From<AMQPFrame> for ServerProperties {
    fn from(fr: AMQPFrame) -> ServerProperties {
        match fr {
            AMQPFrame::Method(channel, cm, args) =>
                ServerProperties {
                    channel: channel,
                    properties: vec![]
                },
            _ =>
                panic!("Conversion error")
        }
    }
}

impl From<ClientProperties> for AMQPFrame {
    fn from(cp: ClientProperties) -> AMQPFrame {
        AMQPFrame::Method(0, 0, Box::new(vec![]))
    }
}

impl From<AMQPFrame> for TuneProperties {
    fn from(fr: AMQPFrame) -> TuneProperties {
        match fr {
            AMQPFrame::Method(channel, cm, args) =>
                TuneProperties {
                },
            _ =>
                panic!("Conversion error")
        }
    }
}

impl From<TuneProperties> for AMQPFrame {
    fn from(tp: TuneProperties) -> AMQPFrame {
        AMQPFrame::Method(0, 0, Box::new(vec![]))
    }
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
