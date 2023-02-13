use std::collections::HashMap;

use metalmq_codec::{
    codec::Frame,
    frame::{self, BasicPublishArgs, BasicPublishFlags, ContentHeaderFrame},
};
use tokio::sync::{mpsc, oneshot};

use crate::{
    client::{state::Connection, ConnectionError},
    exchange, queue,
    tests::recv_timeout,
    ErrorScope, Result, RuntimeError,
};

struct ConnectionTest {
    connection: Connection,
    frame_rx: mpsc::Receiver<Frame>,
}

impl ConnectionTest {
    fn new() -> Self {
        let em = exchange::manager::start();
        let qm = queue::manager::start(em.clone());
        // Buffer is larger than 1 in order not to block main thread.
        let (frame_tx, frame_rx) = mpsc::channel(16);

        ConnectionTest {
            connection: Connection {
                id: "conn-1".to_string(),
                qm,
                em,
                channel_max: 127,
                frame_max: 32_768,
                heartbeat_interval: None,
                open_channels: HashMap::new(),
                exchanges: HashMap::new(),
                auto_delete_exchanges: vec![],
                consumed_queues: HashMap::new(),
                exclusive_queues: vec![],
                passively_consumed_queues: HashMap::new(),
                in_flight_contents: HashMap::new(),
                outgoing: frame_tx,
            },
            frame_rx,
        }
    }

    async fn queue_declare(&mut self, channel: u16, name: &str, exclusive: bool) -> Result<()> {
        let mut flags = frame::QueueDeclareFlags::default();
        flags.set(frame::QueueDeclareFlags::EXCLUSIVE, exclusive);

        let args = frame::QueueDeclareArgs {
            name: name.to_string(),
            flags,
            ..Default::default()
        };

        self.connection.queue_declare(channel, args).await
    }
}

#[ignore]
#[tokio::test]
async fn sending_mismatched_content_header() {
    let mut ct = ConnectionTest::new();

    let basic_publish = BasicPublishArgs {
        exchange_name: "stock-price".to_string(),
        routing_key: "*".to_string(),
        flags: BasicPublishFlags::empty(),
    };
    ct.connection.basic_publish(1, basic_publish).await.unwrap();

    assert!(ct.connection.in_flight_contents.contains_key(&1));

    let content_header = ContentHeaderFrame {
        channel: 1,
        class_id: 61, // non basic.* class
        ..Default::default()
    };
    let res = ct.connection.receive_content_header(content_header).await;

    assert!(res.is_err());
    let err = res.unwrap_err().downcast::<RuntimeError>().unwrap();
    assert_eq!(err.code, ConnectionError::FrameError as u16);
    assert_eq!(err.scope, ErrorScope::Connection);
    // TODO we need to have classes as constants
    assert_eq!(err.class_method, frame::BASIC_PUBLISH);
}

#[tokio::test]
async fn on_connection_close_deletes_exclusive_queues() {
    let mut ct = ConnectionTest::new();

    ct.queue_declare(1, "q-exclusive", true).await.unwrap();

    ct.connection
        .connection_close(frame::ConnectionCloseArgs {
            code: 200,
            text: "Normal close".to_string(),
            class_id: 0x0A,
            method_id: 0x32,
        })
        .await
        .unwrap();

    let channel_closed_ok_frame = recv_timeout(&mut ct.frame_rx).await;

    let (tx, rx) = oneshot::channel();
    ct.connection
        .qm
        .send(queue::manager::QueueManagerCommand::GetQueueSink(
            queue::manager::GetQueueSinkQuery {
                channel: 1,
                queue_name: "q-exclusive".to_string(),
            },
            tx,
        ))
        .await
        .unwrap();

    let queue_sink = rx.await;

    assert!(queue_sink.unwrap().is_err());
}

// TODO send zero body size trigger sending content to the exchange or queue
// TODO send too large content
// TODO exclusive queue should be deleted when client closes the connection
