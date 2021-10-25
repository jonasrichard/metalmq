use self::client_api::{ClientRequest, Param, WaitFor};
use super::*;
use metalmq_codec::codec::Frame;
use metalmq_codec::frame::{self, AMQPFrame, MethodFrameArgs};
use tokio::sync::mpsc;

#[tokio::test]
async fn connect_frame_exchange() {
    env_logger::builder().is_test(true).try_init().unwrap();

    let (frame_out_tx, mut frame_out_rx) = mpsc::channel(16);
    let cs = self::state::new(frame_out_tx);

    let (frame_in_tx, frame_in_rx) = mpsc::channel(16);
    let frames_stream = FrameStream { frames: frame_in_rx };

    let (command_tx, command_rx) = mpsc::channel(1);

    tokio::spawn(async move {
        let result = loop2(cs, Box::pin(frames_stream), command_rx).await;

        assert!(result.is_ok());
    });

    let (connected_tx, connected_rx) = oneshot::channel();
    let cmd_result = command_tx
        .send(ClientRequest {
            param: Param::Connect {
                username: "guest".to_owned(),
                password: "guest".to_owned(),
                virtual_host: "/".to_owned(),
                connected: connected_tx,
            },
            response: WaitFor::Nothing,
        })
        .await;

    log::info!("Header command send result: {:?}", cmd_result);
    assert!(matches!(cmd_result, Ok(())));

    let header = frame_out_rx.recv().await.unwrap();
    assert!(matches!(header, Frame::Frame(AMQPFrame::Header)));

    // Server send back a connection-start
    frame_in_tx
        .send(Frame::Frame(frame::connection_start(0)))
        .await
        .unwrap();

    // Client send back a connection-start-ok
    let conn_start_ok = extract_method_frame(frame_out_rx.recv().await);
    assert!(matches!(conn_start_ok, MethodFrameArgs::ConnectionStartOk(_)));

    if let MethodFrameArgs::ConnectionStartOk(args) = conn_start_ok {
        assert_eq!(args.mechanism, "PLAIN");
    }

    // Server send back a connection-tune
    frame_in_tx.send(Frame::Frame(frame::connection_tune(0))).await.unwrap();

    // Client send back a connection-tune-ok
    let conn_tune_ok = extract_method_frame(frame_out_rx.recv().await);
    assert!(matches!(conn_tune_ok, MethodFrameArgs::ConnectionTuneOk(_)));

    if let MethodFrameArgs::ConnectionTuneOk(args) = conn_tune_ok {
        assert_eq!(args.channel_max, 2047);
    }

    // Client opening a virtual host with connection-open
    let conn_open = extract_method_frame(frame_out_rx.recv().await);
    assert!(matches!(conn_open, MethodFrameArgs::ConnectionOpen(_)));

    frame_in_tx
        .send(Frame::Frame(frame::connection_open_ok(0)))
        .await
        .unwrap();

    let sleep = tokio::time::sleep(tokio::time::Duration::from_secs(1));
    tokio::pin!(sleep);

    tokio::select! {
        _ = connected_rx => {
        }
        _ = &mut sleep => {
            panic!("Connected signal have not arrived");
        }
    }
}

fn extract_method_frame(r: Option<Frame>) -> frame::MethodFrameArgs {
    assert!(r.is_some());

    let frame = r.unwrap();

    if let Frame::Frame(AMQPFrame::Method(ch, cid, args)) = frame {
        args
    } else {
        panic!("Method frame expected {:?}", frame);
    }
}

/// Test frame stream. Autotests can send Frames to the channel and this channel
/// is converted to a futures.Stream which is mocking the SplitStream in the
/// processor.ps.
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
