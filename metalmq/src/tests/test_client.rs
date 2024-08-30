use metalmq_codec::{
    codec::Frame,
    frame::{self, AMQPFrame, ContentBodyFrame, ContentHeaderFrame},
};
use tokio::sync::mpsc;

use crate::client::connection::types::Connection;

/// The test client used by test cases. It makes available the sender part of the input channel, so
/// one can control the connection by sending frames in the `conn_tx`.
pub struct TestClient {
    pub connection: Connection,
    pub conn_tx: mpsc::Sender<Frame>,
    pub conn_rx: mpsc::Receiver<Frame>,
}

impl TestClient {
    pub async fn connect(&mut self) {
        let _connection_start = self.send_frame_with_response(frame::AMQPFrame::Header).await;
        let _connection_tune = self
            .send_frame_with_response(frame::ConnectionStartOkArgs::new("guest", "guest").frame())
            .await;

        self.send_frame(frame::connection_tune_ok()).await;
        let _connection_open_ok = self
            .send_frame_with_response(
                frame::ConnectionOpenArgs {
                    virtual_host: "/".into(),
                    insist: false,
                }
                .frame(),
            )
            .await;
    }

    pub async fn open_channel(&mut self, channel: u16) {
        self.connection
            .handle_client_frame(frame::channel_open(channel))
            .await
            .unwrap();
        self.conn_rx.recv().await.unwrap();
    }

    /// Send frame to client state machine.
    pub async fn send_frame(&mut self, f: AMQPFrame) {
        self.connection.handle_client_frame(f).await.unwrap();
    }

    /// Send frame to client state machine and wait for the response frame.
    pub async fn send_frame_with_response(&mut self, f: AMQPFrame) -> AMQPFrame {
        self.connection.handle_client_frame(f).await.unwrap();

        self.recv_single_frame().await
    }

    pub async fn exchange_declare(&mut self, channel: u16, args: frame::ExchangeDeclareArgs) {
        self.connection.handle_client_frame(args.frame(channel)).await.unwrap();
    }

    pub async fn exchange_delete(&mut self, channel: u16, args: frame::ExchangeDeleteArgs) {
        self.connection.handle_client_frame(args.frame(channel)).await.unwrap();
    }

    pub async fn queue_declare(&mut self, channel: u16, args: frame::QueueDeclareArgs) {
        self.connection.handle_client_frame(args.frame(channel)).await.unwrap();
    }

    pub async fn queue_delete(&mut self, channel: u16, args: frame::QueueDeleteArgs) {
        self.connection.handle_client_frame(args.frame(channel)).await.unwrap();
    }

    pub async fn basic_publish(&mut self, channel: u16, args: frame::BasicPublishArgs) {
        self.connection.handle_client_frame(args.frame(channel)).await.unwrap();
    }

    pub async fn basic_consume(&mut self, channel: u16, args: frame::BasicConsumeArgs) {
        let f = args.frame(channel);

        self.connection.handle_client_frame(f).await.unwrap();
    }

    pub async fn basic_ack(&mut self, channel: u16, args: frame::BasicAckArgs) {
        self.connection.handle_client_frame(args.frame(channel)).await.unwrap();
    }

    pub async fn basic_get(&mut self, channel: u16, args: frame::BasicGetArgs) {
        self.connection.handle_client_frame(args.frame(channel)).await.unwrap();
    }

    pub async fn basic_cancel(&mut self, channel: u16, args: frame::BasicCancelArgs) {
        let f = args.frame(channel);

        self.connection.handle_client_frame(f).await.unwrap();
    }

    pub async fn publish_content(&mut self, channel: u16, exchange: &str, routing_key: &str, message: &[u8]) {
        let f = frame::BasicPublishArgs::new(exchange)
            .routing_key(routing_key)
            .frame(channel);

        self.connection.handle_client_frame(f).await.unwrap();

        self.send_content(channel, message).await;
    }

    pub async fn send_content(&mut self, channel: u16, message: &[u8]) {
        let header = ContentHeaderFrame {
            channel,
            class_id: (frame::BASIC_PUBLISH >> 16) as u16,
            body_size: message.len() as u64,
            ..Default::default()
        };

        self.connection
            .handle_client_frame(AMQPFrame::ContentHeader(header))
            .await
            .unwrap();

        let body = ContentBodyFrame {
            channel,
            body: message.to_vec(),
        };

        self.connection
            .handle_client_frame(AMQPFrame::ContentBody(body))
            .await
            .unwrap();
    }

    /// Receiving with timeout
    pub async fn recv_timeout(&mut self) -> Option<Frame> {
        let sleep = tokio::time::sleep(tokio::time::Duration::from_secs(1));
        tokio::pin!(sleep);

        tokio::select! {
            frame = self.conn_rx.recv() => {
                frame
            }
            _ = &mut sleep => {
                None
            }
        }
    }

    pub async fn recv_frames(&mut self) -> Vec<frame::AMQPFrame> {
        unpack_frames(self.recv_timeout().await.expect("At least one frame is expected"))
    }

    pub async fn recv_single_frame(&mut self) -> frame::AMQPFrame {
        unpack_single_frame(self.recv_timeout().await.expect("A frame is expected"))
    }

    pub async fn close(&mut self) {
        self.connection.close().await.unwrap();
    }

    pub async fn close_channel(&mut self, channel: u16) {
        self.connection.close_channel(channel).await.unwrap();
    }
}

pub async fn sleep(ms: u32) {
    tokio::time::sleep(std::time::Duration::from_millis(ms.into())).await;
}

pub fn unpack_single_frame(f: Frame) -> frame::AMQPFrame {
    if let Frame::Frame(single_frame) = f {
        single_frame
    } else {
        panic!("Frame {f:?} is not a single frame");
    }
}

pub fn unpack_frames(f: Frame) -> Vec<frame::AMQPFrame> {
    match f {
        Frame::Frame(sf) => vec![sf],
        Frame::Frames(mf) => mf,
    }
}

// It should be an assert
pub fn basic_deliver_args(f: frame::AMQPFrame) -> frame::BasicDeliverArgs {
    if let frame::AMQPFrame::Method(_, _, frame::MethodFrameArgs::BasicDeliver(args)) = f {
        return args;
    }

    panic!("Not a BasicDeliver frame");
}
