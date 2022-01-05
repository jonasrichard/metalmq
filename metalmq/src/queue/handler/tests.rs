use super::*;
use metalmq_codec::codec::Frame;

fn default_queue_state() -> QueueState {
    let q = Queue {
        name: "test-queue".to_string(),
        ..Default::default()
    };

    QueueState {
        queue: q,
        declaring_connection: "conn-id".to_string(),
        messages: VecDeque::new(),
        outbox: Outbox {
            outgoing_messages: vec![],
        },
        bound_exchanges: HashSet::new(),
        candidate_consumers: vec![],
        consumers: vec![],
        next_consumer: 0,
    }
}
async fn recv_timeout(rx: &mut mpsc::Receiver<Frame>) -> Option<Frame> {
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
async fn publish_to_queue_without_consumers() {
    let mut qs = default_queue_state();

    let cmd = QueueCommand::PublishMessage(Box::new(crate::message::tests::empty_message()));

    let result = qs.handle_command(cmd).await;
    assert!(result.is_ok());
    assert_eq!(qs.messages.len(), 1);

    let msg = qs.messages.get(0).unwrap();
    assert_eq!(msg.source_connection, "conn-id");
    assert_eq!(msg.channel, 1);
    assert_eq!(msg.content.body, b"");
}

#[tokio::test]
async fn publish_to_queue_with_one_consumer() {
    use metalmq_codec::codec::Frame;
    use metalmq_codec::frame::{self, AMQPFrame};

    let mut qs = default_queue_state();
    let (msg_tx, mut msg_rx) = mpsc::channel(1);
    let (tx, rx) = oneshot::channel();

    let cmd = QueueCommand::StartConsuming {
        conn_id: "consumer-conn".to_string(),
        consumer_tag: "myctag".to_string(),
        channel: 1,
        no_ack: false,
        exclusive: false,
        sink: msg_tx,
        result: tx,
    };

    let result = qs.handle_command(cmd).await;
    assert!(result.is_ok());
    assert_eq!(qs.candidate_consumers.len(), 1);

    let cmd_result = rx.await;
    assert!(cmd_result.is_ok());

    let cmd = QueueCommand::StartDelivering {
        conn_id: "consumer-conn".to_string(),
        channel: 1,
        consumer_tag: "myctag".to_string(),
    };
    let result = qs.handle_command(cmd).await;
    assert!(result.is_ok());
    assert_eq!(qs.consumers.len(), 1);

    let cmd = QueueCommand::PublishMessage(Box::new(crate::message::tests::empty_message()));

    let result = qs.handle_command(cmd).await;
    assert!(result.is_ok());

    let frame = recv_timeout(&mut msg_rx).await.unwrap();
    if let Frame::Frames(fs) = frame {
        if let AMQPFrame::Method(ch, cm, frame::MethodFrameArgs::BasicDeliver(args)) = fs.get(0).unwrap() {
            assert_eq!(ch, &1);
            assert_eq!(cm, &frame::BASIC_DELIVER);
            assert_eq!(args.consumer_tag, "myctag");
            assert_eq!(args.delivery_tag, 1);
        }
        // TODO check the other frames
    } else {
        println!("{:?}", frame);
        assert!(false, "Basic.Delivery frame is expected");
    }
}
