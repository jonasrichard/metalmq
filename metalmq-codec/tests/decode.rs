use bytes::BytesMut;
use futures::stream::StreamExt;
use metalmq_codec::codec;
use metalmq_codec::frame;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use tokio_util::codec::{Encoder, Framed, FramedParts};

#[derive(Debug)]
struct MockStream {
    index: usize,
    pending: bool,
    chunk_size: usize,
    bytes: Vec<u8>,
    waker: Option<Waker>,
}

impl tokio::io::AsyncRead for MockStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        println!("{:?}", self);

        if self.pending {
            self.pending = false;
            self.waker = Some(cx.waker().clone());

            return Poll::Pending;
        }

        if self.index == self.bytes.len() {
            return Poll::Ready(io::Result::Ok(()));
        }

        let end = std::cmp::min(self.index + self.chunk_size, self.bytes.len());
        println!("end = {}", end);

        buf.put_slice(&self.bytes[self.index..end]);
        self.index = end;

        println!("buf = {:?}", buf);

        Poll::Ready(io::Result::Ok(()))
    }
}

#[tokio::test]
async fn can_read_from_existing_buf() {
    let mut cdc = metalmq_codec::codec::AMQPCodec {};
    let mut buffer = BytesMut::new();

    let res = cdc.encode(
        codec::Frame::Frame(frame::ConnectionStartArgs::new().frame()),
        &mut buffer,
    );

    assert!(res.is_ok());

    let stream = MockStream {
        index: 0,
        pending: false,
        chunk_size: 16,
        bytes: buffer.to_vec(),
        waker: None,
    };
    let mut parts = FramedParts::new(stream, codec::AMQPCodec {});
    parts.read_buf = BytesMut::from(buffer);

    let mut framed = Framed::from_parts(parts);
    let res = framed.next().await;

    assert!(res.is_some());

    //println!("res = {:?}", res.unwrap());
}
