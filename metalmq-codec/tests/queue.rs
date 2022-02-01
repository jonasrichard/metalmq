use bytes::BytesMut;
use metalmq_codec::codec::{AMQPCodec, Frame};
use metalmq_codec::frame::{self, AMQPFrame};
use tokio_util::codec::{Decoder, Encoder};

macro_rules! extract_method_frame_args {
    ( $name:ident, $frame:expr ) => {
        match $frame {
            Frame::Frame(sf) => match sf {
                AMQPFrame::Method(_, _, mf) => match mf {
                    frame::MethodFrameArgs::$name(args) => args,
                    _ => panic!("{:?} is not of the correct type", mf),
                },
                _ => panic!("{:?} is not a method frame", sf),
            },
            _ => panic!("It is not a single frame"),
        }
    };
}

#[test]
fn queue_purge() {
    let mut codec = AMQPCodec {};
    let mut buf = BytesMut::with_capacity(128);

    let fr = frame::queue_purge(2, "test-queue");
    assert!(codec.encode(Frame::Frame(fr), &mut buf).is_ok());

    let res = codec.decode(&mut buf);
    assert!(res.is_ok());

    let maybe_frame = res.unwrap();
    assert!(maybe_frame.is_some());

    let purge_frame = maybe_frame.unwrap();
    let args = extract_method_frame_args!(QueuePurge, purge_frame);
    assert_eq!(args.queue_name, "test-queue");
}
