use bencher::Bencher;
use bytes::BytesMut;
use ironmq_codec::frame::{AMQPFieldValue, AMQPFrame, AMQPValue, MethodFrame};
use std::collections::HashMap;
use tokio_util::codec::Encoder;

fn generate_frame() -> AMQPFrame {
    let mut sub_fields = HashMap::<String, AMQPFieldValue>::new();
    sub_fields.insert("another long string".into(), AMQPFieldValue::LongString("the value".into()));

    let mut fields = HashMap::<String, AMQPFieldValue>::new();
    fields.insert("long string".into(), AMQPFieldValue::LongString("A long string".into()));
    fields.insert("bool value".into(), AMQPFieldValue::Bool(true));
    fields.insert("".into(), AMQPFieldValue::FieldTable(Box::new(sub_fields)));

    let args = vec![
        AMQPValue::U16(4000),
        AMQPValue::SimpleString("This is a benchmark string".into()),
        AMQPValue::LongString("A long string has length info 4 bytes long".into()),
        AMQPValue::FieldTable(Box::new(fields))
    ];

    AMQPFrame::Method(Box::new(MethodFrame {
        channel: 12,
        class_method: 0x1100000A,
        args: args
    }))
}

fn method_frame(bench: &mut Bencher) {
    let mut codec = ironmq_codec::codec::AMQPCodec{};

    bench.iter(move || {
        let mut buf = BytesMut::with_capacity(1024);
        let frame = generate_frame();

        codec.encode(frame, &mut buf)
    });
}

bencher::benchmark_group!(encoder,
    method_frame);

bencher::benchmark_main!(encoder);
