use tokio::sync::mpsc;

use crate::{
    client::{state::Connection, tests::to_runtime_error, ConnectionError},
    exchange::manager::ExchangeManagerSink,
    message::Message,
    queue::manager::QueueManagerSink,
    tests::recv_timeout,
    Context,
};
use metalmq_codec::{
    codec::Frame,
    frame::{
        self, BasicPublishArgs, ContentBodyFrame, ContentHeaderFrame, ExchangeDeclareArgs, QueueBindArgs,
        QueueDeclareArgs,
    },
};

/// TestCase for System Under Test which spawns an exchange manager and a queue manager and can
/// test more integrated features like forwarding messages from exchanges to queues.
pub struct TestCase {
    em: ExchangeManagerSink,
    qm: QueueManagerSink,
}

impl TestCase {
    fn new() -> Self {
        let em = crate::exchange::manager::start();
        let qm = crate::queue::manager::start(em.clone());

        Self { em, qm }
    }

    fn new_client(&self) -> (Connection, mpsc::Receiver<Frame>) {
        let ctx = Context {
            exchange_manager: self.em.clone(),
            queue_manager: self.qm.clone(),
        };
        let (client_tx, client_rx) = mpsc::channel(1);

        (Connection::new(ctx, client_tx), client_rx)
    }
}

pub async fn send_content(client: &mut Connection, message: &[u8]) {
    let mut header = ContentHeaderFrame::default();
    header.channel = 1u16;
    header.class_id = (frame::BASIC_PUBLISH >> 16) as u16;
    header.body_size = message.len() as u64;

    client.receive_content_header(header).await.unwrap();

    let body = ContentBodyFrame {
        channel: 1u16,
        body: message.to_vec(),
    };

    client.receive_content_body(body).await.unwrap();
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

#[tokio::test]
async fn bind_queue_with_validation() {
    let tc = TestCase::new();
    let (mut client, mut client_rx) = tc.new_client();

    // Normal exchange declaration sends back ExchangeDeclareOk
    let args = ExchangeDeclareArgs::default()
        .exchange_name("normal-exchange")
        .exchange_type("direct");
    client.exchange_declare(1u16, args).await.unwrap();

    let exchange_declare_ok = unpack_single_frame(recv_timeout(&mut client_rx).await.unwrap());

    assert!(matches!(exchange_declare_ok, frame::AMQPFrame::Method(_, _, _)));

    // Declaring reserved exchanges ends up in channel error
    let args = ExchangeDeclareArgs::default()
        .exchange_name("amq.reserved")
        .exchange_type("direct");
    client.exchange_declare(2u16, args).await.unwrap();

    let channel_error = unpack_single_frame(recv_timeout(&mut client_rx).await.unwrap());

    assert!(matches!(
        channel_error,
        frame::AMQPFrame::Method(
            2u16,
            _,
            frame::MethodFrameArgs::ChannelClose(frame::ChannelCloseArgs { code: 403, .. })
        )
    ));

    // Declaring invalid exchange e.g. empty name ends up in connection error
    let args = ExchangeDeclareArgs::default();
    let result = client.exchange_declare(3u16, args).await;

    let channel_error = to_runtime_error(result);

    assert_eq!(channel_error.code, ConnectionError::CommandInvalid as u16);
}

#[tokio::test]
async fn basic_publish_mandatory_message() {
    let tc = TestCase::new();
    let (mut client, mut client_rx) = tc.new_client();

    let args = ExchangeDeclareArgs::default()
        .exchange_name("mandatory")
        .exchange_type("direct");
    client.exchange_declare(1u16, args).await.unwrap();
    // ExchangeDeclareOk
    recv_timeout(&mut client_rx).await.unwrap();

    // Publish message to an exchange which doesn't route to queues -> channel error
    let publish = BasicPublishArgs::new("mandatory").mandatory(true);

    client.basic_publish(1u16, publish).await.unwrap();

    send_content(&mut client, b"A simple message").await;

    // Since there is no queue bound and message is mandatory, server sends back the message with a
    // Basic.Return frame
    let return_frames = unpack_frames(recv_timeout(&mut client_rx).await.unwrap());

    assert!(matches!(
        dbg!(return_frames.get(0).unwrap()),
        frame::AMQPFrame::Method(1u16, _, frame::MethodFrameArgs::BasicReturn(_))
    ));

    let args = QueueDeclareArgs::default().name("mandatory-queue");
    client.queue_declare(2u16, args).await.unwrap();
    // QueueDeclareOk
    recv_timeout(&mut client_rx).await.unwrap();

    let args = QueueBindArgs::new("mandatory-queue", "mandatory");
    client.queue_bind(2u16, args).await.unwrap();
    // QueueBindOk
    recv_timeout(&mut client_rx).await.unwrap();

    send_content(&mut client, b"Another message").await;

    // No message is expected as a response
    let expected_timeout = recv_timeout(&mut client_rx).await;
    assert!(expected_timeout.is_none());
}
