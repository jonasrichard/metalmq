use bencher::Bencher;
use bytes::BytesMut;
use metalmq_codec::codec::Frame;
use metalmq_codec::frame::{self, AMQPFrame, MethodFrameArgs};
use tokio_util::codec::Encoder;

fn generate_frame() -> Frame {
    let args = frame::QueueDeclareArgs {
        name: "test queue".into(),
        ..Default::default()
    };

    Frame::Frame(AMQPFrame::Method(
        12,
        frame::QUEUE_DECLARE,
        MethodFrameArgs::QueueDeclare(args),
    ))
}

fn method_frame(bench: &mut Bencher) {
    let mut codec = metalmq_codec::codec::AMQPCodec {};

    bench.iter(move || {
        let frame = generate_frame();
        let mut buf = BytesMut::with_capacity(1024);

        codec.encode(frame, &mut buf)
    });
}

bencher::benchmark_group!(encoder, method_frame);

bencher::benchmark_main!(encoder);
