use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};

#[derive(Debug)]
pub enum AMQPFrame {
    Method,
}

pub struct AMQPCodec {
}

impl Encoder<AMQPFrame> for AMQPCodec {
    type Error = std::io::Error;

    fn encode(&mut self, event: AMQPFrame, buf: &mut BytesMut) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl Decoder for AMQPCodec {
    type Item = AMQPFrame;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        Ok(Some(AMQPFrame::Method))
    }
}
