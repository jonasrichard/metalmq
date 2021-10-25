use self::client_api::{ClientRequest, Param, WaitFor};
use super::*;
use futures::stream::{self, StreamExt};
use metalmq_codec::codec::Frame;
use metalmq_codec::frame::AMQPFrame;
use tokio::sync::mpsc;

#[tokio::test]
async fn connect() {
    let (frame_tx, frame_rx) = mpsc::channel(16);
    let cs = self::state::new(frame_tx);

    let frames_rx = FrameStream { frames: frame_rrx };

    let (command_tx, command_rx) = mpsc::channel(1);

    let result = loop2(cs, Box::pin(frames_rx), command_rx).await;

    assert!(result.is_ok());

    let cmd_result = command_tx
        .send(ClientRequest {
            param: Param::Frame(AMQPFrame::Header),
            response: WaitFor::Nothing,
        })
        .await;

    println!("Header command send result: {:?}", cmd_result);
    assert!(matches!(cmd_result, Ok(())));

    frame_tx.send();
}

struct FrameStream {
    frames: mpsc::Receiver<Frame>,
}

use futures::task::{Context, Poll};

impl futures::stream::Stream for FrameStream {
    type Item = Result<Frame, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, mut cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.frames.poll_recv(&mut cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(val)) => Poll::Ready(Some(Ok(val))),
        }
    }
}
