mod consume;
mod publish;
mod queue;

use std::collections::HashMap;

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
struct TestCase {
    em: ExchangeManagerSink,
    qm: QueueManagerSink,
    setup_tx: mpsc::Sender<Frame>,
    setup_rx: mpsc::Receiver<Frame>,
}

impl TestCase {
    /// Create the internal engine of MetalMQ, the exchange and the queue manager, it connects them
    /// and also declare all types of exchanges for tests.
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

        self.queue_bind("q-direct", "x-direct", "magic-key", None).await;
        self.queue_bind("q-fanout", "x-fanout", "", None).await;
        self.queue_bind("q-topic", "x-topic", "topic.#", None).await;

        let mut args: HashMap<String, frame::AMQPFieldValue> = HashMap::new();
        args.insert("x-match".into(), frame::AMQPFieldValue::LongString("any".into()));
        args.insert(
            "message.type".into(),
            frame::AMQPFieldValue::LongString("string".into()),
        );

        self.queue_bind("q-headers", "x-headers", "", Some(args)).await;

        while self.setup_rx.try_recv().is_ok() {}

        self
    }

    async fn teardown(self) {
        self.queue_delete("q-direct").await;
        self.exchange_delete("x-direct").await;
        self.queue_delete("q-fanout").await;
        self.exchange_delete("x-fanout").await;
        self.queue_delete("q-topic").await;
        self.exchange_delete("x-topic").await;
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

    async fn exchange_delete(&self, name: &str) {
        em::delete_exchange(
            &self.em,
            em::DeleteExchangeCommand {
                channel: 1,
                if_unused: false,
                exchange_name: name.to_string(),
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

    async fn queue_bind(
        &self,
        queue_name: &str,
        exchange_name: &str,
        routing_key: &str,
        args: Option<frame::FieldTable>,
    ) {
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
                args,
                queue_sink: sink,
            },
        )
        .await
        .unwrap();
    }

    async fn queue_delete(&self, queue_name: &str) {
        qm::delete_queue(
            &self.qm,
            qm::QueueDeleteCommand {
                conn_id: "does-not-matter".to_string(),
                channel: 1,
                queue_name: queue_name.to_string(),
                if_unused: false,
                if_empty: false,
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
        let (client_tx, client_rx) = mpsc::channel(16);

        (Connection::new(ctx, client_tx), client_rx)
    }
}

async fn channel_close(client: &mut Connection, channel: u16) {
    client
        .channel_close(channel, frame::ChannelCloseArgs::default())
        .await
        .unwrap();
}

async fn connection_close(client: &mut Connection) {
    client
        .connection_close(frame::ConnectionCloseArgs::default())
        .await
        .unwrap();
}

async fn publish_content(client: &mut Connection, channel: u16, exchange: &str, routing_key: &str, message: &[u8]) {
    client
        .basic_publish(channel, frame::BasicPublishArgs::new(exchange).routing_key(routing_key))
        .await
        .unwrap();
    send_content(client, channel, message).await;
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
            None
        }
    }
}

async fn send_content(client: &mut Connection, channel: u16, message: &[u8]) {
    let header = ContentHeaderFrame {
        channel,
        class_id: (frame::BASIC_PUBLISH >> 16) as u16,
        body_size: message.len() as u64,
        ..Default::default()
    };

    client.receive_content_header(header).await.unwrap();

    let body = ContentBodyFrame {
        channel,
        body: message.to_vec(),
    };

    client.receive_content_body(body).await.unwrap();
}

async fn sleep(ms: u32) {
    tokio::time::sleep(std::time::Duration::from_millis(ms.into())).await;
}

async fn recv_frames(client: &mut mpsc::Receiver<Frame>) -> Vec<frame::AMQPFrame> {
    unpack_frames(recv_timeout(client).await.unwrap())
}

async fn recv_single_frame(client: &mut mpsc::Receiver<Frame>) -> frame::AMQPFrame {
    unpack_single_frame(recv_timeout(client).await.unwrap())
}

fn unpack_single_frame(f: Frame) -> frame::AMQPFrame {
    if let Frame::Frame(single_frame) = f {
        single_frame
    } else {
        panic!("Frame {f:?} is not a single frame");
    }
}

fn unpack_frames(f: Frame) -> Vec<frame::AMQPFrame> {
    match f {
        Frame::Frame(sf) => vec![sf],
        Frame::Frames(mf) => mf,
    }
}

fn basic_deliver_args(f: frame::AMQPFrame) -> frame::BasicDeliverArgs {
    if let frame::AMQPFrame::Method(_, _, frame::MethodFrameArgs::BasicDeliver(args)) = f {
        return args;
    }

    panic!("Not a BasicDeliver frame");
}
