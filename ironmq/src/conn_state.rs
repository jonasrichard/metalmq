use crate::Result;
use ironmq_codec::frame::{AMQPFrame, MethodFrame};

pub(crate) struct ConnectionState {
    virtual_host: String,
}

pub(crate) trait Connection {
    fn connection_open(&mut self, mf: MethodFrame) -> Result<Option<AMQPFrame>>;
}

impl Connection for ConnectionState {
    fn connection_open(&mut self, mf: MethodFrame) -> Result<Option<AMQPFrame>> {
        mf.args.get(0)
    }
}
