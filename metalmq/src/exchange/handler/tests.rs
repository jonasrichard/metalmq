use super::QueueBindCmd;
use crate::{
    error::{to_runtime_error, ChannelError, ErrorScope, Result},
    exchange::{
        binding::Bindings,
        handler::{ExchangeCommand, ExchangeState, QueueUnbindCmd},
        Exchange, ExchangeType,
    },
    message::{Message, MessageContent},
    queue::{
        handler::{self, QueueCommand, QueueCommandSink},
        Queue,
    },
};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{mpsc, oneshot};

struct TestCase {
    connection_id: String,
    channel: u16,
    exchange_state: ExchangeState,
}

impl TestCase {
    fn direct_exchange(name: &str) -> Self {
        Self {
            connection_id: "default-id".to_string(),
            channel: 1u16,
            exchange_state: ExchangeState {
                exchange: Exchange {
                    name: name.to_string(),
                    exchange_type: ExchangeType::Direct,
                    durable: false,
                    auto_delete: false,
                    internal: false,
                },
                bindings: Bindings::Direct(vec![]),
                bound_queues: HashMap::new(),
            },
        }
    }

    fn message(&self, exchange_name: &str, body: &str) -> Message {
        Message {
            source_connection: self.connection_id.clone(),
            channel: self.channel,
            content: MessageContent {
                body: body.into(),
                ..Default::default()
            },
            exchange: exchange_name.to_string(),
            routing_key: "".to_string(),
            mandatory: false,
            immediate: false,
        }
    }

    fn command_message(&self, message: Message, tx: oneshot::Sender<Option<Arc<Message>>>) -> ExchangeCommand {
        ExchangeCommand::Message {
            message,
            returned: Some(tx),
        }
    }

    fn command_bind(
        &self,
        queue: &str,
        routing_key: &str,
        queue_sink: QueueCommandSink,
        tx: oneshot::Sender<Result<bool>>,
    ) -> QueueBindCmd {
        QueueBindCmd {
            conn_id: self.connection_id.clone(),
            channel: self.channel,
            queue_name: queue.to_string(),
            routing_key: routing_key.to_string(),
            args: None,
            sink: queue_sink,
            result: tx,
        }
    }
}

#[tokio::test]
async fn send_basic_return_on_mandatory_unroutable_message() {
    let exchange_name: String = String::from("x-name");

    let (msg_tx, msg_rx) = oneshot::channel();
    let mut tc = TestCase::direct_exchange(&exchange_name);

    let mut msg = tc.message(&exchange_name, "Okay");
    msg.mandatory = true;

    let cmd = tc.command_message(msg, msg_tx);

    tc.exchange_state.handle_command(cmd).await.unwrap();

    let returned_message = msg_rx.await.unwrap().unwrap();

    assert_eq!(returned_message.exchange, "x-name");
    // TODO more checks
}

#[tokio::test]
async fn cannot_bind_nonexisting_queue() {
    let exchange_name = String::from("x-404");
    let mut tc = TestCase::direct_exchange(&exchange_name);

    let (q_tx, q_rx) = mpsc::channel(1);
    let (r_tx, r_rx) = oneshot::channel();

    // Simulate the situation when queue had been closed already, but the reference to the sink we
    // still have.
    drop(q_rx);

    let cmd = tc.command_bind("non-existing-queue", "", q_tx, r_tx);

    tc.exchange_state
        .handle_command(ExchangeCommand::QueueBind(cmd))
        .await
        .unwrap();

    let result = r_rx.await.unwrap();
    assert!(result.is_err());

    let err = to_runtime_error(result.unwrap_err());
    assert_eq!(ChannelError::NotFound as u16, err.code);
}

#[tokio::test]
async fn cannot_bind_exclusive_queue_with_different_connection() {
    let exchange_name = String::from("x-input");
    let queue_name = String::from("exclusive-queue");
    let mut tc = TestCase::direct_exchange(&exchange_name);

    let mut queue = Queue::default();
    queue.name = queue_name.clone();
    queue.exclusive = true;

    let stx = queue_start("conn-id-1".to_string(), &queue);

    let (tx, rx) = oneshot::channel();
    let mut cmd = tc.command_bind(&queue_name, "routing", stx, tx);
    cmd.conn_id = "different-id".to_string();

    tc.exchange_state
        .handle_command(ExchangeCommand::QueueBind(cmd))
        .await
        .unwrap();

    let res = rx.await.unwrap();
    assert!(res.is_err());

    let err = to_runtime_error(res.unwrap_err());
    assert_eq!(err.scope, ErrorScope::Channel);
    assert_eq!(err.code, ChannelError::ResourceLocked as u16);
}

#[tokio::test]
async fn queue_bind_state_check() {
    let exchange_name = String::from("normal-exchange");
    let queue_name = String::from("normal-queue");
    let connection_id = String::from("conn-id-1");
    let mut tc = TestCase::direct_exchange(&exchange_name);

    let mut queue = Queue::default();
    queue.name = queue_name.clone();

    let stx = queue_start(connection_id.clone(), &queue);

    let (tx, rx) = oneshot::channel();
    let mut cmd = tc.command_bind(&queue_name, "routing", stx, tx);
    cmd.conn_id = connection_id.clone();

    tc.exchange_state
        .handle_command(ExchangeCommand::QueueBind(cmd))
        .await
        .unwrap();

    assert!(rx.await.is_ok());

    let bq = tc.exchange_state.bound_queues.get(&queue_name);
    assert!(bq.is_some());
    assert_eq!(bq.unwrap().queue_name, queue_name);
    assert_eq!(bq.unwrap().declaring_connection, connection_id);

    if let Bindings::Direct(bs) = tc.exchange_state.bindings {
        assert!(bs.iter().position(|b| b.queue_name == queue_name).is_some());
    } else {
        panic!();
    }
}

#[tokio::test]
async fn queue_bind_unbind_state_check() {
    let exchange_name = String::from("normal-exchange");
    let queue_name = String::from("normal-queue");
    let connection_id = String::from("conn-id-1");

    let mut tc = TestCase::direct_exchange(&exchange_name);
    tc.connection_id = connection_id.clone();

    let mut queue = Queue::default();
    queue.name = queue_name.clone();

    let stx = queue_start(connection_id.clone(), &queue);

    let (tx, rx) = oneshot::channel();
    let cmd = tc.command_bind(&queue_name, "routing", stx, tx);

    tc.exchange_state
        .handle_command(ExchangeCommand::QueueBind(cmd))
        .await
        .unwrap();

    assert!(rx.await.is_ok());

    let (tx, rx) = oneshot::channel();
    let cmd = ExchangeCommand::QueueUnbind(QueueUnbindCmd {
        channel: 2,
        queue_name: "normal-queue".to_string(),
        routing_key: "routing".to_string(),
        result: tx,
    });

    tc.exchange_state.handle_command(cmd).await.unwrap();

    let res2 = rx.await;
    assert!(res2.is_ok());
    assert!(res2.unwrap().unwrap());

    assert!(!tc.exchange_state.bound_queues.contains_key("normal-queue"));
}

#[tokio::test]
async fn queue_deleted_state_check() {
    let exchange_name = String::from("normal-exchange");
    let queue_name = String::from("normal-queue");

    let mut tc = TestCase::direct_exchange(&exchange_name);

    let mut queue = Queue::default();
    queue.name = queue_name.clone();

    let (tx, rx) = oneshot::channel();
    let cmd = ExchangeCommand::QueueDeleted {
        queue_name: queue_name.clone(),
        result: tx,
    };

    tc.exchange_state.handle_command(cmd).await.unwrap();

    rx.await.unwrap();

    assert!(!tc.exchange_state.bound_queues.contains_key(&queue_name));

    if let Bindings::Direct(bs) = tc.exchange_state.bindings {
        assert!(bs.iter().position(|b| b.queue_name == queue_name).is_none());
    } else {
        panic!();
    }
}

/// Start the queue thread with the declaring connection and the queue record.
fn queue_start(conn_id: String, queue: &Queue) -> mpsc::Sender<QueueCommand> {
    let (tx, mut rx) = mpsc::channel(1);
    let q = queue.clone();

    tokio::spawn(async move {
        handler::start(q, conn_id, &mut rx).await;
    });

    tx
}
