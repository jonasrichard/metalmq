use crate::Result;
use ironmq_codec::frame;
use ironmq_codec::frame::AMQPFrame;

pub(crate) struct ConnectionState {
    pub(crate) virtual_host: String,
}

pub(crate) trait Connection {
    fn connection_open(&mut self, args: frame::ConnectionOpenArgs) -> Result<Option<AMQPFrame>>;
}

impl Default for ConnectionState {
    fn default() -> Self {
        ConnectionState {
            virtual_host: "".into()
        }
    }
}

impl Connection for ConnectionState {
    fn connection_open(&mut self, args: frame::ConnectionOpenArgs) -> Result<Option<AMQPFrame>> {
        Ok(None)
    }
}
