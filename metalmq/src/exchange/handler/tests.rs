use crate::{
    client::ChannelError,
    exchange::{
        binding::Bindings,
        handler::{ExchangeCommand, ExchangeState},
        Exchange, ExchangeType,
    },
    message::{Message, MessageContent},
    queue::{
        handler::{self, QueueCommand},
        Queue,
    },
    ErrorScope, RuntimeError,
};
use metalmq_codec::{codec::Frame, frame};
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

async fn recv_timeout<T>(rx: &mut mpsc::Receiver<T>) -> Option<T> {
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

#[tokio::test]
async fn send_basic_return_on_mandatory_unroutable_message() {
    let (msg_tx, mut msg_rx) = mpsc::channel(1);
    let mut es = exchange_state_direct("x-name");

    let msg = Message {
        source_connection: "conn-id".to_string(),
        channel: 2,
        content: MessageContent {
            body: b"Okay".to_vec(),
            ..Default::default()
        },
        exchange: "x-name".to_string(),
        routing_key: "".to_string(),
        mandatory: true,
        immediate: false,
    };
    let cmd = ExchangeCommand::Message {
        message: msg,
        frame_size: 32_768,
        outgoing: msg_tx,
    };
    let res = es.handle_command(cmd).await;
    assert!(res.is_ok());

    match recv_timeout(&mut msg_rx).await {
        Some(Frame::Frame(_br)) => assert!(true),
        Some(Frame::Frames(fs)) => if let frame::AMQPFrame::Method(_ch, _cm, _args) = fs.get(0).unwrap() {},
        None => assert!(false, "Basic.Return frame is expected"),
    }
}

#[tokio::test]
async fn cannot_bind_exclusive_queue_with_different_connection() {
    let mut es = exchange_state_direct("xchg-exclusive");
    let mut queue = Queue::default();

    queue.name = "exclusive-queue".to_string();
    queue.exclusive = true;

    let stx = queue_start("conn-id-1".to_string(), &queue);

    let (tx, rx) = oneshot::channel();
    let cmd = ExchangeCommand::QueueBind {
        conn_id: "123".to_string(),
        channel: 1,
        queue_name: "exclusive-queue".to_string(),
        routing_key: "routing".to_string(),
        args: None,
        sink: stx,
        result: tx,
    };

    let res = es.handle_command(cmd).await;
    assert!(res.is_ok());

    let res2 = rx.await;
    assert!(res2.is_ok());
    let res3 = res2.unwrap();
    assert!(res3.is_err());

    let err = res3.unwrap_err().downcast::<RuntimeError>().unwrap();
    assert_eq!(err.scope, ErrorScope::Channel);
    assert_eq!(err.code, ChannelError::ResourceLocked as u16);
}

#[tokio::test]
async fn queue_bind_state_check() {
    let mut es = exchange_state_direct("x-name");
    let mut queue = Queue::default();

    queue.name = "normal-queue".to_string();

    let stx = queue_start("conn-id-1".to_string(), &queue);

    let (tx, rx) = oneshot::channel();
    let cmd = ExchangeCommand::QueueBind {
        conn_id: "conn-id-1".to_string(),
        channel: 1,
        queue_name: "normal-queue".to_string(),
        routing_key: "routing".to_string(),
        args: None,
        sink: stx,
        result: tx,
    };

    let res = es.handle_command(cmd).await;
    assert!(res.is_ok());
    assert!(rx.await.is_ok());

    let bq = es.bound_queues.get("normal-queue");
    assert!(bq.is_some());
    assert_eq!(bq.unwrap().queue_name, "normal-queue");
    assert_eq!(bq.unwrap().declaring_connection, "conn-id-1");

    if let Bindings::Direct(bs) = es.bindings {
        assert!(bs.iter().position(|b| b.queue_name == "normal-queue").is_some());
    } else {
        panic!();
    }
}

#[tokio::test]
async fn queue_bind_unbind_state_check() {
    let mut es = exchange_state_direct("x-name");
    let mut queue = Queue::default();

    queue.name = "normal-queue".to_string();

    let stx = queue_start("conn-id-1".to_string(), &queue);

    let (tx, rx) = oneshot::channel();
    let cmd = ExchangeCommand::QueueBind {
        conn_id: "conn-id-1".to_string(),
        channel: 1,
        queue_name: "normal-queue".to_string(),
        routing_key: "routing".to_string(),
        args: None,
        sink: stx,
        result: tx,
    };

    let res = es.handle_command(cmd).await;
    assert!(res.is_ok());
    assert!(rx.await.is_ok());

    let (tx, rx) = oneshot::channel();
    let cmd = ExchangeCommand::QueueUnbind {
        channel: 2,
        queue_name: "normal-queue".to_string(),
        routing_key: "routing".to_string(),
        result: tx,
    };

    let res = es.handle_command(cmd).await;
    assert!(res.is_ok());

    let res2 = rx.await;
    assert!(res2.is_ok());
    assert!(res2.unwrap().unwrap());

    assert!(!es.bound_queues.contains_key("normal-queue"));
}

#[tokio::test]
async fn queue_deleted_state_check() {
    let mut es = exchange_state_direct("x-name");
    let mut queue = Queue::default();

    queue.name = "bound-queue".to_string();

    let (tx, rx) = oneshot::channel();
    let cmd = ExchangeCommand::QueueDeleted {
        queue_name: "bound-queue".to_string(),
        result: tx,
    };

    es.handle_command(cmd).await.unwrap();

    rx.await.unwrap();

    assert!(!es.bound_queues.contains_key("bound-queue"));

    if let Bindings::Direct(bs) = es.bindings {
        assert!(bs.iter().position(|b| b.queue_name == "bound-queue").is_none());
    } else {
        panic!();
    }
}

/// Create the exchange state holder for a direct exchange.
fn exchange_state_direct(name: &str) -> ExchangeState {
    ExchangeState {
        exchange: Exchange {
            name: name.to_string(),
            exchange_type: ExchangeType::Direct,
            durable: false,
            auto_delete: false,
            internal: false,
        },
        bindings: Bindings::Direct(vec![]),
        bound_queues: HashMap::new(),
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
