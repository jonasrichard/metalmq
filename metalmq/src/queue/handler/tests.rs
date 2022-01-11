use super::*;
use crate::message::{Message, MessageContent};
use metalmq_codec::codec::Frame;
use std::sync::Arc;

struct TestCase {
    connection_id: String,
    used_channel: u16,
    exchange_name: String,
    queue_name: String,
    message_body: String,
    message_mandatory: bool,
}

impl Default for TestCase {
    fn default() -> Self {
        TestCase {
            connection_id: "id-12345".to_owned(),
            used_channel: 1u16,
            exchange_name: "my-exchange".to_owned(),
            queue_name: "my-queue".to_owned(),
            message_body: "Hey, buddy!".to_owned(),
            message_mandatory: false,
        }
    }
}

impl TestCase {
    fn default_queue_state(&self) -> QueueState {
        let q = Queue {
            name: self.queue_name.clone(),
            ..Default::default()
        };

        QueueState {
            queue: q,
            declaring_connection: self.connection_id.clone(),
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

    fn default_message(&self) -> Message {
        let body = self.message_body.clone().into_bytes();
        let body_len = body.len() as u64;

        Message {
            source_connection: self.connection_id.clone(),
            channel: self.used_channel,
            exchange: self.exchange_name.clone(),
            routing_key: "".to_owned(),
            mandatory: self.message_mandatory,
            immediate: false,
            content: MessageContent {
                body,
                body_size: body_len,
                content_type: Some("text/plain".to_owned()),
                content_encoding: Some("utf-8".to_owned()),
                ..Default::default()
            },
        }
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

/// If messages is published to a queue and there are no consumers,
/// the queue should store the message.
#[tokio::test]
async fn publish_to_queue_without_consumers() {
    let test_case = TestCase::default();
    let message = test_case.default_message();
    let mut qs = test_case.default_queue_state();

    let cmd = QueueCommand::PublishMessage(Arc::new(message.clone()));

    let result = qs.handle_command(cmd).await;
    assert!(result.is_ok());
    assert_eq!(qs.messages.len(), 1);

    let msg = qs.messages.get(0).unwrap();
    assert_eq!(msg.source_connection, message.source_connection);
    assert_eq!(msg.channel, message.channel);
    assert_eq!(msg.content.body, message.content.body);
}

/// If message is published to a queue and there is a consumer,
/// the queue is passed to the consumer.
#[tokio::test]
async fn publish_to_queue_with_one_consumer() {
    use metalmq_codec::codec::Frame;
    use metalmq_codec::frame::{self, AMQPFrame};

    let test_case = TestCase::default();
    let message = test_case.default_message();
    let mut qs = test_case.default_queue_state();

    let (msg_tx, mut msg_rx) = mpsc::channel(1);
    let (tx, rx) = oneshot::channel();

    let cmd = QueueCommand::StartConsuming {
        conn_id: test_case.connection_id.clone(),
        consumer_tag: "myctag".to_string(),
        channel: test_case.used_channel,
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
        conn_id: test_case.connection_id.clone(),
        channel: test_case.used_channel,
        consumer_tag: "myctag".to_string(),
    };
    let result = qs.handle_command(cmd).await;
    assert!(result.is_ok());
    assert_eq!(qs.consumers.len(), 1);

    let cmd = QueueCommand::PublishMessage(Arc::new(message));

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
