use std::time::Duration;

use tokio::sync::mpsc;

use crate::{
    client::{state::Connection, tests::to_runtime_error, ConnectionError},
    exchange::{
        manager::{self as em, ExchangeManagerSink},
        Exchange, ExchangeType,
    },
    queue::{
        manager::{self as qm, QueueManagerSink},
        Queue,
    },
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
    setup_tx: mpsc::Sender<Frame>,
    setup_rx: mpsc::Receiver<Frame>,
}

impl TestCase {
    async fn new() -> Self {
        let em = crate::exchange::manager::start();
        let qm = crate::queue::manager::start(em.clone());
        let (setup_tx, setup_rx) = mpsc::channel(128);

        Self {
            em,
            qm,
            setup_tx,
            setup_rx,
        }
        .setup()
        .await
    }

    async fn setup(mut self) -> Self {
        self.exchange_declare("x-direct", ExchangeType::Direct).await;
        self.exchange_declare("x-fanout", ExchangeType::Fanout).await;
        self.exchange_declare("x-topic", ExchangeType::Topic).await;
        self.exchange_declare("x-headers", ExchangeType::Headers).await;

        self.queue_declare("q-direct").await;
        self.queue_declare("q-fanout").await;
        self.queue_declare("q-topic").await;
        self.queue_declare("q-headers").await;

        self.queue_bind("q-direct", "x-direct", "magic-key").await;
        self.queue_bind("q-fanout", "x-fanout", "").await;

        while let Ok(_) = self.setup_rx.try_recv() {}

        self
    }

    async fn exchange_declare(&self, name: &str, exchange_type: ExchangeType) {
        em::declare_exchange(
            &self.em,
            em::DeclareExchangeCommand {
                channel: 1,
                exchange: Exchange::default().name(name).exchange_type(exchange_type),
                passive: false,
                outgoing: self.setup_tx.clone(),
            },
        )
        .await
        .unwrap();
    }

    async fn queue_declare(&self, name: &str) {
        qm::declare_queue(
            &self.qm,
            qm::QueueDeclareCommand {
                queue: Queue::default().name(name),
                conn_id: "does-not-matter".to_string(),
                channel: 1,
                passive: false,
            },
        )
        .await
        .unwrap();
    }

    async fn queue_bind(&self, queue_name: &str, exchange_name: &str, routing_key: &str) {
        let sink = qm::get_command_sink(
            &self.qm,
            qm::GetQueueSinkQuery {
                channel: 1,
                queue_name: queue_name.to_string(),
            },
        )
        .await
        .unwrap();

        em::bind_queue(
            &self.em,
            em::BindQueueCommand {
                conn_id: "does-not-matter".to_string(),
                channel: 1,
                exchange_name: exchange_name.to_string(),
                queue_name: queue_name.to_string(),
                routing_key: routing_key.to_string(),
                args: None,
                queue_sink: sink,
            },
        )
        .await
        .unwrap();
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
    let tc = TestCase::new().await;
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
    let tc = TestCase::new().await;
    let (mut client, mut client_rx) = tc.new_client();

    // Publish message to an exchange which doesn't route to queues -> channel error
    let publish = BasicPublishArgs::new("x-direct")
        .routing_key("invalid-key")
        .mandatory(true);

    client.basic_publish(1u16, publish).await.unwrap();

    send_content(&mut client, b"A simple message").await;

    // Since the routing key is not matching and message is mandatory, server sends back the message
    // with a Basic.Return frame
    let return_frames = unpack_frames(recv_timeout(&mut client_rx).await.unwrap());

    assert!(matches!(
        dbg!(return_frames).get(0).unwrap(),
        frame::AMQPFrame::Method(1u16, _, frame::MethodFrameArgs::BasicReturn(_))
    ));

    client
        .basic_publish(
            1u16,
            BasicPublishArgs::new("x-direct")
                .routing_key("magic-key")
                .mandatory(true),
        )
        .await
        .unwrap();

    send_content(&mut client, b"Another message").await;

    // No message is expected as a response
    let expected_timeout = dbg!(recv_timeout(&mut client_rx).await);
    assert!(expected_timeout.is_none());
}

#[tokio::test]
async fn basic_get_empty_and_ok() {
    let tc = TestCase::new().await;
    let (mut client, mut client_rx) = tc.new_client();

    client
        .basic_get(2, frame::BasicGetArgs::new("q-fanout").no_ack(false))
        .await
        .unwrap();

    let get_empty_frames = unpack_frames(recv_timeout(&mut client_rx).await.unwrap());

    assert!(matches!(
        dbg!(get_empty_frames).get(0).unwrap(),
        frame::AMQPFrame::Method(2, _, frame::MethodFrameArgs::BasicGetEmpty)
    ));

    client
        .basic_publish(1, frame::BasicPublishArgs::new("x-fanout"))
        .await
        .unwrap();
    send_content(&mut client, b"A fanout message").await;

    tokio::time::sleep(Duration::from_millis(100)).await;

    client.basic_get(2, frame::BasicGetArgs::new("q-fanout")).await.unwrap();

    let get_frames = unpack_frames(recv_timeout(&mut client_rx).await.unwrap());

    assert!(matches!(
        dbg!(get_frames).get(0).unwrap(),
        frame::AMQPFrame::Method(
            2,
            _,
            frame::MethodFrameArgs::BasicGetOk(frame::BasicGetOkArgs { redelivered: false, .. })
        )
    ));
}
