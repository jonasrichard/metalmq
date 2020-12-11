use bencher::Bencher;
use bytes::BytesMut;
use ironmq_codec::frame;
use ironmq_codec::frame::{AMQPFrame, MethodFrameArgs};
use tokio_util::codec::Encoder;

fn generate_frame() -> AMQPFrame {
    let args = frame::QueueDeclareArgs {
        name: "test queue".into(),
        ..Default::default()
    };

    AMQPFrame::Method(12, frame::QUEUE_DECLARE, MethodFrameArgs::QueueDeclare(args))
}

fn method_frame(bench: &mut Bencher) {
    let mut codec = ironmq_codec::codec::AMQPCodec {};

    bench.iter(move || {
        let mut buf = BytesMut::with_capacity(1024);
        let frame = generate_frame();

        codec.encode(frame, &mut buf)
    });
}

bencher::benchmark_group!(encoder, method_frame);

bencher::benchmark_main!(encoder);
