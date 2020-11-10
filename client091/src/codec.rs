use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

#[derive(Debug)]
pub enum AMQPFrame {
    AMQPHeader,
    Method,
}

pub struct AMQPCodec {
}

impl Encoder<AMQPFrame> for AMQPCodec {
    type Error = std::io::Error;

    fn encode(&mut self, event: AMQPFrame, buf: &mut BytesMut) -> Result<(), Self::Error> {
        match event {
            AMQPFrame::AMQPHeader =>
                buf.put(&b"AMQP\x00\x00\x09\x01"[..]),

            AMQPFrame::Method => {
            }
        }
        Ok(())
    }
}

impl Decoder for AMQPCodec {
    type Item = AMQPFrame;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 8 {
            Ok(None)
        } else {
            match src.get_u8() {
                1 => {
                    let _channel = src.get_u16();
                    // TODO amqp frame can be u32 but Buf handles only usize buffes
                    let frame_len = src.get_u32() as usize;

                    if src.len() < frame_len {
                        return Ok(None)
                    }

                    let mut frame_buf = src.split_to(frame_len);
                    let frame = decode_method_frame(&mut frame_buf);

                    Ok(Some(frame))
                },
                f =>
                    panic!("Unknown frame {}", f)
            }
        }
    }
}

// TODO have an Error type here, and it should be result<>
fn decode_method_frame(src: &mut BytesMut) -> AMQPFrame {
    let class = src.get_u16();
    let method = src.get_u16();
    let version_major = src.get_u8();
    let version_minor = src.get_u8();

    AMQPFrame::Method
}
