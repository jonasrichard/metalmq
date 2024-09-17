use metalmq_codec::{
    codec::Frame,
    frame::{self, AMQPFrame, ContentBodyFrame, ContentHeaderFrame},
};
use tokio::{sync::mpsc, task::JoinHandle};

use super::recv;
use crate::error::Result;

/// The test client used by test cases. It makes available the sender part of the input channel, so
/// one can control the connection by sending frames in the `conn_tx`.
pub struct TestClient {
    pub connection: JoinHandle<Result<()>>,
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

        self.assert_no_extra_frames().await;
    }

    pub async fn open_channel(&mut self, channel: u16) -> AMQPFrame {
        self.send_frame_with_response(frame::channel_open(channel)).await
    }

    pub async fn exchange_declare(&mut self, channel: u16, args: frame::ExchangeDeclareArgs) -> AMQPFrame {
        self.send_frame_with_response(args.frame(channel)).await
    }

    pub async fn exchange_delete(&mut self, channel: u16, args: frame::ExchangeDeleteArgs) -> AMQPFrame {
        self.send_frame_with_response(args.frame(channel)).await
    }

    pub async fn queue_declare(&mut self, channel: u16, args: frame::QueueDeclareArgs) -> AMQPFrame {
        self.send_frame_with_response(args.frame(channel)).await
    }

    pub async fn queue_delete(&mut self, channel: u16, args: frame::QueueDeleteArgs) -> AMQPFrame {
        self.send_frame_with_response(args.frame(channel)).await
    }

    pub async fn basic_publish(&mut self, channel: u16, args: frame::BasicPublishArgs) {
        self.send_frame(args.frame(channel)).await;
    }

    pub async fn basic_consume(&mut self, channel: u16, args: frame::BasicConsumeArgs) -> AMQPFrame {
        self.send_frame_with_response(args.frame(channel)).await
    }

    pub async fn basic_ack(&mut self, channel: u16, args: frame::BasicAckArgs) {
        self.send_frame(args.frame(channel)).await;
    }

    pub async fn basic_get(&mut self, channel: u16, args: frame::BasicGetArgs) {
        self.send_frame(args.frame(channel)).await;
    }

    pub async fn basic_cancel(&mut self, channel: u16, args: frame::BasicCancelArgs) -> AMQPFrame {
        self.send_frame_with_response(args.frame(channel)).await
    }

    pub async fn confirm_select(&mut self, channel: u16) -> AMQPFrame {
        self.send_frame_with_response(frame::confirm_select(channel)).await
    }

    pub async fn publish_content(&mut self, channel: u16, exchange: &str, routing_key: &str, message: &[u8]) {
        let f = frame::BasicPublishArgs::new(exchange)
            .routing_key(routing_key)
            .frame(channel);

        self.send_frame(f).await;

        self.send_content(channel, message).await;
    }

    pub async fn send_content(&mut self, channel: u16, message: &[u8]) {
        let header = ContentHeaderFrame {
            channel,
            class_id: (frame::BASIC_PUBLISH >> 16) as u16,
            body_size: message.len() as u64,
            ..Default::default()
        };

        self.send_frame(AMQPFrame::ContentHeader(header)).await;

        let body = ContentBodyFrame {
            channel,
            body: message.to_vec(),
        };

        self.send_frame(AMQPFrame::ContentBody(body)).await;
    }

    /// Send frame to client state machine.
    pub async fn send_frame(&mut self, f: AMQPFrame) {
        self.conn_tx.send(f.into()).await.unwrap();
    }

    /// Send frame to client state machine and wait for the response frame.
    pub async fn send_frame_with_response(&mut self, f: AMQPFrame) -> AMQPFrame {
        self.send_frame(f).await;
        self.recv_single_frame().await
    }

    /// Receiving with timeout
    pub async fn recv_timeout(&mut self) -> Option<Frame> {
        recv::recv_with_timeout(&mut self.conn_rx).await
    }

    pub async fn recv_frames(&mut self) -> Vec<frame::AMQPFrame> {
        recv::recv_multiple_frames(&mut self.conn_rx).await
    }

    pub async fn recv_single_frame(&mut self) -> frame::AMQPFrame {
        recv::recv_single_frame(&mut self.conn_rx).await
    }

    pub async fn close(&mut self) {
        self.send_frame_with_response(frame::connection_close(200, "Normal close", frame::CONNECTION_CLOSE))
            .await;
    }

    pub async fn close_channel(&mut self, channel: u16) {
        self.send_frame_with_response(frame::channel_close(channel, 200, "Normal close", frame::CHANNEL_CLOSE))
            .await;
    }

    async fn assert_no_extra_frames(&mut self) {
        if !self.conn_rx.is_empty() {
            while let Ok(f) = self.conn_rx.try_recv() {
                dbg!(f);
            }

            panic!("Extra frames in the receiver");
        }
    }
}

pub async fn sleep(ms: u32) {
    tokio::time::sleep(std::time::Duration::from_millis(ms.into())).await;
}

// It should be an assert
pub fn basic_deliver_args(f: frame::AMQPFrame) -> frame::BasicDeliverArgs {
    if let frame::AMQPFrame::Method(_, _, frame::MethodFrameArgs::BasicDeliver(args)) = f {
        return args;
    }

    panic!("Not a BasicDeliver frame");
}
