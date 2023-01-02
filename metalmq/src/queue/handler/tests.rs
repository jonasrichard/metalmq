use super::*;
use crate::message::{Message, MessageContent};
use metalmq_codec::{codec::Frame, frame::AMQPFrame};
use std::sync::Arc;

struct TestCase {
    connection_id: String,
    used_channel: u16,
    exchange_name: String,
    queue_name: String,
    message_mandatory: bool,
}

impl Default for TestCase {
    fn default() -> Self {
        TestCase {
            connection_id: "id-12345".to_owned(),
            used_channel: 1u16,
            exchange_name: "my-exchange".to_owned(),
            queue_name: "my-queue".to_owned(),
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

    fn default_message(&self, body: &str) -> Message {
        let body = body.to_owned().as_bytes().to_vec();
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

    fn command_consuming(&self, ctag: &str, ftx: FrameSink, rtx: oneshot::Sender<Result<()>>) -> QueueCommand {
        QueueCommand::StartConsuming {
            conn_id: self.connection_id.clone(),
            channel: self.used_channel,
            consumer_tag: ctag.to_string(),
            no_ack: false,
            exclusive: false,
            sink: ftx,
            frame_size: 1024,
            result: rtx,
        }
    }

    fn command_start_delivering(&self, ctag: &str) -> QueueCommand {
        QueueCommand::StartDelivering {
            consumer_tag: ctag.to_string(),
        }
    }

    fn command_cancel_consume(&self, ctag: &str, rtx: oneshot::Sender<bool>) -> QueueCommand {
        QueueCommand::CancelConsuming {
            consumer_tag: ctag.to_string(),
            result: rtx,
        }
    }

    fn command_publish(&self, body: &str) -> QueueCommand {
        QueueCommand::PublishMessage(Arc::new(self.default_message(body)))
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

fn parse_message(frame: Frame) -> Option<Message> {
    let mut message = Message { ..Default::default() };

    if let Frame::Frames(fs) = frame {
        for f in fs {
            match f {
                AMQPFrame::Method(
                    channel,
                    frame::BASIC_DELIVER,
                    frame::MethodFrameArgs::BasicDeliver(frame::BasicDeliverArgs {
                        exchange_name,
                        routing_key,
                        ..
                    }),
                ) => {
                    message.channel = channel;
                    message.exchange = exchange_name;
                    message.routing_key = routing_key;
                }
                AMQPFrame::ContentHeader(frame::ContentHeaderFrame { body_size, .. }) => {
                    message.content.body_size = body_size;
                }
                AMQPFrame::ContentBody(frame::ContentBodyFrame { body, .. }) => {
                    message.content.body = body;
                }
                _ => (),
            }
        }

        Some(message)
    } else {
        return None;
    }
}

/// If messages is published to a queue and there are no consumers,
/// the queue should store the message.
#[tokio::test]
async fn publish_to_queue_without_consumers() {
    let test_case = TestCase::default();
    let message = test_case.default_message("Hey, man");
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
    let message = test_case.default_message("Hey, man");
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
        frame_size: 32_768,
        result: tx,
    };

    let result = qs.handle_command(cmd).await;
    assert!(result.is_ok());
    assert_eq!(qs.candidate_consumers.len(), 1);

    let cmd_result = rx.await;
    assert!(cmd_result.is_ok());

    let cmd = QueueCommand::StartDelivering {
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

#[tokio::test]
async fn cannot_delete_non_empty_queue_if_empty_true() {
    use crate::RuntimeError;

    let test_case = TestCase::default();
    let message = test_case.default_message("Hey, man");
    let mut qs = test_case.default_queue_state();

    qs.handle_command(QueueCommand::PublishMessage(Arc::new(message)))
        .await
        .unwrap();

    let (em_tx, _em_rx) = mpsc::channel(1);
    let (del_tx, del_rx) = oneshot::channel();
    let del_result = qs
        .handle_command(QueueCommand::DeleteQueue {
            conn_id: "123".to_owned(),
            channel: 5u16,
            if_unused: false,
            if_empty: true,
            exchange_manager: em_tx,
            result: del_tx,
        })
        .await
        .unwrap();

    assert_eq!(del_result, true);

    let del_cmd_result = del_rx.await.unwrap();
    assert!(del_cmd_result.is_err());

    let err = del_cmd_result.unwrap_err().downcast::<RuntimeError>().unwrap();
    assert_eq!(err.code, ChannelError::PreconditionFailed as u16);
    assert_eq!(err.text, "Queue is not empty".to_string());

    // we could check if exchange manager gets the unbind command for that queue
}

#[tokio::test]
async fn unacked_messages_should_be_put_back_in_the_queue() {
    let test_case = TestCase::default();
    let mut qs = test_case.default_queue_state();

    let ctag = "ctag-1".to_string();
    let (ftx, mut frx) = mpsc::channel(1);
    let (rtx, rrx) = oneshot::channel();
    qs.handle_command(test_case.command_consuming(&ctag, ftx, rtx))
        .await
        .unwrap();

    rrx.await.unwrap().unwrap();

    qs.handle_command(test_case.command_start_delivering(&ctag))
        .await
        .unwrap();

    qs.handle_command(test_case.command_publish("1st")).await.unwrap();
    let _msg_res = recv_timeout(&mut frx).await.unwrap();

    qs.handle_command(test_case.command_publish("2nd")).await.unwrap();
    let _msg_res = recv_timeout(&mut frx).await.unwrap();

    //let message = parse_message(msg_res);

    let (rtx, rrx) = oneshot::channel();
    qs.handle_command(test_case.command_cancel_consume(&ctag, rtx))
        .await
        .unwrap();

    rrx.await.unwrap();

    // Test the original order of two messages not just one

    assert_eq!(qs.messages.len(), 2);

    let message1 = qs.messages.pop_front().unwrap();
    assert_eq!(message1.content.body, b"1st");

    let message2 = qs.messages.pop_front().unwrap();
    assert_eq!(message2.content.body, b"2nd");
}

// TODO when a consumer cancel consuming on an exclusive queue, the queue should be deleted
