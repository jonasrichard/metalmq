pub mod publish;
pub mod sut;

use tokio::sync::mpsc;

use crate::{
    client::state::Connection,
    exchange::{
        manager::{self as em, ExchangeManagerSink},
        Exchange, ExchangeType,
    },
    queue::{
        manager::{self as qm, QueueManagerSink},
        Queue,
    },
    Context,
};
use metalmq_codec::{
    codec::Frame,
    frame::{self, ContentBodyFrame, ContentHeaderFrame},
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

/// Receiving with timeout
pub async fn recv_timeout<T>(rx: &mut mpsc::Receiver<T>) -> Option<T> {
    let sleep = tokio::time::sleep(tokio::time::Duration::from_secs(1));
    tokio::pin!(sleep);

    tokio::select! {
        frame = rx.recv() => {
            frame
        }
        _ = &mut sleep => {
            return None;
        }
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
