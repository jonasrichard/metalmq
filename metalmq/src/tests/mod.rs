mod connect;
mod consume;
mod publish;
mod queue;
pub mod recv;
mod test_client;

use std::collections::HashMap;

use tokio::sync::mpsc;

use crate::{
    client::connection::types::Connection,
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
use metalmq_codec::{codec::Frame, frame};
use test_client::TestClient;

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

    /// Create a new client and return the outgoing channel part as well.
    fn new_client(&self) -> TestClient {
        let ctx = Context {
            exchange_manager: self.em.clone(),
            queue_manager: self.qm.clone(),
        };
        let (conn_tx, conn_rx) = mpsc::channel(16);

        TestClient {
            connection: Connection::new(ctx, conn_tx.clone()),
            conn_tx,
            conn_rx,
        }
    }

    async fn new_client_with_channel(&self, channel: u16) -> TestClient {
        let mut client = self.new_client();

        client.connect().await;
        client.open_channel(channel).await;

        client
    }

    async fn new_client_with_channels(&self, channels: &[u16]) -> TestClient {
        let mut client = self.new_client();

        client.connect().await;

        for channel in channels {
            client.open_channel(*channel).await;
        }

        client
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
}
