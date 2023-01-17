use super::*;
use bytes::{Buf, BufMut, BytesMut};
use codec::{AMQPCodec, Frame};
use frame::{AMQPFrame, MethodFrameArgs};
use tokio_util::codec::Encoder;

#[test]
fn encode_header_frame() {
    let mut encoder = AMQPCodec {};
    let mut buf = BytesMut::with_capacity(1024);

    let res = encoder.encode(Frame::Frame(AMQPFrame::Header), &mut buf);

    assert!(res.is_ok());

    let expected = b"AMQP\x00\x00\x09\x01";
    let mut current = [0u8; 8];

    buf.copy_to_slice(&mut current[..]);

    assert_eq!(expected, &current);
}

#[test]
fn encode_method_frame() {
    let mut encoder = AMQPCodec {};
    let mut buf = BytesMut::with_capacity(1024);

    let args = frame::QueueBindArgs {
        queue_name: "queue".into(),
        exchange_name: "exchg".into(),
        routing_key: "key".into(),
        no_wait: false,
        args: None,
    };

    let res = encoder.encode(
        Frame::Frame(AMQPFrame::Method(
            0x0205,
            frame::QUEUE_BIND,
            MethodFrameArgs::QueueBind(args),
        )),
        &mut buf,
    );

    assert!(res.is_ok());

    let frame_header = b"\x01\x02\x05";
    let class_method = b"\x00\x32\x00\x14";

    let mut argbuf = BytesMut::with_capacity(256);
    argbuf.put(&class_method[..]);
    argbuf.put(&b"\x00\x00"[..]);
    argbuf.put(&b"\x05queue"[..]);
    argbuf.put(&b"\x05exchg"[..]);
    argbuf.put(&b"\x03key"[..]);
    argbuf.put(&b"\x00"[..]);
    argbuf.put(&b"\x00\x00\x00\x00"[..]);

    let mut expected = BytesMut::with_capacity(256);
    expected.put(&frame_header[..]);
    expected.put_u32(argbuf.len() as u32);
    expected.put(argbuf);
    expected.put_u8(0xCE);

    assert_eq!(expected, buf);
}
