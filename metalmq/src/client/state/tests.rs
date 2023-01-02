use std::collections::HashMap;

use metalmq_codec::{
    codec::Frame,
    frame::{BasicPublishArgs, BasicPublishFlags, ContentHeaderFrame},
};
use tokio::sync::mpsc;

use crate::{
    client::{state::Connection, ConnectionError},
    exchange, queue, ErrorScope, RuntimeError,
};

struct ConnectionTest {
    connection: Connection,
    frame_rx: mpsc::Receiver<Frame>,
}

impl ConnectionTest {
    fn new() -> Self {
        let em = exchange::manager::start();
        let qm = queue::manager::start(em.clone());
        let (frame_tx, frame_rx) = mpsc::channel(1);

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
                in_flight_contents: HashMap::new(),
                outgoing: frame_tx,
            },
            frame_rx,
        }
    }
}

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
    assert_eq!(err.class_id, 60);
}

// TODO send zero body size trigger sending content to the exchange or queue
// TODO send too large content
