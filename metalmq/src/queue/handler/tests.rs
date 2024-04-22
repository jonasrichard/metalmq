use super::*;
use crate::{
    client::tests::to_runtime_error,
    message::{Message, MessageContent},
    tests::recv_timeout,
    ErrorScope,
};
use metalmq_codec::{codec::Frame, frame::AMQPFrame};
use std::sync::Arc;

struct TestCase {
    connection_id: String,
    used_channel: u16,
    exchange_name: String,
    queue_name: String,
}

impl Default for TestCase {
    fn default() -> Self {
        TestCase {
            connection_id: "id-12345".to_owned(),
            used_channel: 1u16,
            exchange_name: "my-exchange".to_owned(),
            queue_name: "my-queue".to_owned(),
        }
    }
}

impl TestCase {
    fn with_queue(self, name: &str) -> Self {
        Self {
            queue_name: name.to_string(),
            ..self
        }
    }

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
            passive_consumers: vec![],
            next_consumer: 0,
        }
    }

    fn build(self) -> QueueStateTester {
        let qs = self.default_queue_state();

        QueueStateTester {
            connection_id: self.connection_id,
            used_channel: self.used_channel,
            exchange_name: self.exchange_name,
            state: qs,
        }
    }
}

struct QueueStateTester {
    connection_id: String,
    used_channel: u16,
    exchange_name: String,
    state: QueueState,
}

impl QueueStateTester {
    fn default_message(&self, body: &str) -> Message {
        let body = body.to_owned().as_bytes().to_vec();
        let body_len = body.len() as u64;

        Message {
            source_connection: self.connection_id.clone(),
            channel: self.used_channel,
            exchange: self.exchange_name.clone(),
            routing_key: "".to_owned(),
            mandatory: false,
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

    fn command_consuming(
        &self,
        ctag: &str,
        no_ack: bool,
        ftx: FrameSink,
        rtx: oneshot::Sender<Result<()>>,
    ) -> QueueCommand {
        QueueCommand::StartConsuming {
            conn_id: self.connection_id.clone(),
            channel: self.used_channel,
            consumer_tag: ctag.to_string(),
            no_ack,
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

    fn command_basic_ack(
        &self,
        channel: u16,
        consumer_tag: &str,
        delivery_tag: u64,
    ) -> (QueueCommand, oneshot::Receiver<Result<()>>) {
        let (tx, rx) = oneshot::channel();

        (
            QueueCommand::AckMessage(AckCmd {
                channel,
                consumer_tag: consumer_tag.to_string(),
                delivery_tag,
                multiple: false,
                result: tx,
            }),
            rx,
        )
    }

    fn command_passive_cancel(&self) -> QueueCommand {
        QueueCommand::PassiveCancelConsume(PassiveCancelConsumeCmd {
            conn_id: self.connection_id.clone(),
            channel: self.used_channel,
        })
    }

    fn command_delete(
        &self,
        channel: u16,
        if_unused: bool,
        if_empty: bool,
        em: ExchangeManagerSink,
        rtx: oneshot::Sender<Result<u32>>,
    ) -> QueueCommand {
        QueueCommand::DeleteQueue(DeleteQueueCmd {
            conn_id: self.connection_id.clone(),
            channel,
            if_unused,
            if_empty,
            exchange_manager: em,
            result: rtx,
        })
    }

    async fn consume(&mut self, ctag: &str, no_ack: bool) -> mpsc::Receiver<Frame> {
        let (ftx, frx) = mpsc::channel(1);

        let (rtx, rrx) = oneshot::channel();
        self.state
            .handle_command(self.command_consuming(ctag, no_ack, ftx, rtx))
            .await
            .unwrap();
        rrx.await.unwrap().unwrap();

        self.state
            .handle_command(self.command_start_delivering(ctag))
            .await
            .unwrap();

        frx
    }

    async fn passive_consume(&mut self) -> mpsc::Receiver<Frame> {
        let (ftx, frx) = mpsc::channel(1);
        let (rtx, rrx) = oneshot::channel();

        self.state
            .handle_command(QueueCommand::PassiveConsume(PassiveConsumeCmd {
                conn_id: self.connection_id.clone(),
                channel: self.used_channel,
                frame_size: 1024,
                sink: ftx,
                result: rtx,
            }))
            .await
            .unwrap();

        rrx.await.unwrap().unwrap();

        let (rtx, rrx) = oneshot::channel();
        self.state
            .handle_command(QueueCommand::Get(GetCmd {
                conn_id: self.connection_id.clone(),
                channel: self.used_channel,
                no_ack: false,
                result: rtx,
            }))
            .await
            .unwrap();

        rrx.await.unwrap().unwrap();

        frx
    }
}

#[derive(Default)]
struct DeliveredMessage {
    consumer_tag: String,
    delivery_tag: u64,
    redelivered: bool,
    message: Message,
}

fn parse_message(frame: Frame) -> Option<DeliveredMessage> {
    let mut dm = DeliveredMessage { ..Default::default() };

    if let Frame::Frames(fs) = frame {
        for f in fs {
            match f {
                AMQPFrame::Method(
                    channel,
                    frame::BASIC_DELIVER,
                    frame::MethodFrameArgs::BasicDeliver(frame::BasicDeliverArgs {
                        consumer_tag,
                        delivery_tag,
                        redelivered,
                        exchange_name,
                        routing_key,
                    }),
                ) => {
                    dm.message.channel = channel;
                    dm.consumer_tag = consumer_tag;
                    dm.delivery_tag = delivery_tag;
                    dm.redelivered = redelivered;
                    dm.message.exchange = exchange_name;
                    dm.message.routing_key = routing_key;
                }
                AMQPFrame::Method(
                    channel,
                    frame::BASIC_GET_OK,
                    frame::MethodFrameArgs::BasicGetOk(frame::BasicGetOkArgs {
                        delivery_tag,
                        redelivered,
                        exchange_name,
                        routing_key,
                        ..
                    }),
                ) => {
                    dm.message.channel = channel;
                    dm.consumer_tag = String::from("");
                    dm.delivery_tag = delivery_tag;
                    dm.redelivered = redelivered;
                    dm.message.exchange = exchange_name;
                    dm.message.routing_key = routing_key;
                }
                AMQPFrame::ContentHeader(frame::ContentHeaderFrame { body_size, .. }) => {
                    dm.message.content.body_size = body_size;
                }
                AMQPFrame::ContentBody(frame::ContentBodyFrame { body, .. }) => {
                    dm.message.content.body = body;
                }
                _ => (),
            }
        }

        Some(dm)
    } else {
        None
    }
}

/// If messages is published to a queue and there are no consumers,
/// the queue should store the message.
#[tokio::test]
async fn publish_to_queue_without_consumers() {
    let mut tester = TestCase::default().build();
    let message = tester.default_message("Hey, man");

    let cmd = QueueCommand::PublishMessage(Arc::new(message.clone()));

    let result = tester.state.handle_command(cmd).await;
    assert!(result.is_ok());
    assert_eq!(tester.state.messages.len(), 1);

    let msg = tester.state.messages.get(0).unwrap();
    assert_eq!(msg.message.source_connection, message.source_connection);
    assert_eq!(msg.message.channel, message.channel);
    assert_eq!(msg.message.content.body, message.content.body);
}

/// If message is published to a queue and there is a consumer,
/// the queue is passed to the consumer.
#[tokio::test]
async fn publish_to_queue_with_one_consumer() {
    let mut tester = TestCase::default().build();
    let message = tester.default_message("Hey, man");

    let (msg_tx, mut msg_rx) = mpsc::channel(1);
    let (tx, rx) = oneshot::channel();

    let cmd = tester.command_consuming("myctag", false, msg_tx, tx);

    let result = tester.state.handle_command(cmd).await;
    assert!(result.is_ok());
    assert_eq!(tester.state.candidate_consumers.len(), 1);

    let cmd_result = rx.await;
    assert!(cmd_result.is_ok());

    let cmd = QueueCommand::StartDelivering {
        consumer_tag: "myctag".to_string(),
    };
    let result = tester.state.handle_command(cmd).await;
    assert!(result.is_ok());
    assert_eq!(tester.state.consumers.len(), 1);

    let cmd = QueueCommand::PublishMessage(Arc::new(message));

    let result = tester.state.handle_command(cmd).await;
    assert!(result.is_ok());

    let frame = recv_timeout(&mut msg_rx).await.unwrap();

    let message = parse_message(frame).unwrap();
    assert_eq!(message.message.exchange, tester.exchange_name);
}

#[tokio::test]
async fn cannot_delete_non_empty_queue_if_empty_true() {
    let mut tester = TestCase::default().build();
    let message = tester.default_message("Hey, man");

    tester
        .state
        .handle_command(QueueCommand::PublishMessage(Arc::new(message)))
        .await
        .unwrap();

    let (em_tx, _em_rx) = mpsc::channel(1);
    let (del_tx, del_rx) = oneshot::channel();
    let cmd = tester.command_delete(5u16, false, true, em_tx, del_tx);
    let del_result = tester.state.handle_command(cmd).await.unwrap();

    assert!(del_result);

    let del_cmd_result = del_rx.await.unwrap();
    assert!(del_cmd_result.is_err());

    let err = to_runtime_error(del_cmd_result);
    assert_eq!(err.code, ChannelError::PreconditionFailed as u16);
    assert_eq!(err.text, "Queue is not empty".to_string());

    // we could check if exchange manager gets the unbind command for that queue
}

#[tokio::test]
async fn unacked_messages_should_be_put_back_in_the_queue() {
    let mut tester = TestCase::default().build();

    let mut frx = tester.consume("ctag-2", false).await;

    tester
        .state
        .handle_command(tester.command_publish("1st"))
        .await
        .unwrap();
    let _msg_res = recv_timeout(&mut frx).await.unwrap();

    tester
        .state
        .handle_command(tester.command_publish("2nd"))
        .await
        .unwrap();
    let _msg_res = recv_timeout(&mut frx).await.unwrap();

    let (rtx, rrx) = oneshot::channel();
    tester
        .state
        .handle_command(tester.command_cancel_consume("ctag-2", rtx))
        .await
        .unwrap();

    rrx.await.unwrap();

    // Test the original order of two messages not just one

    assert_eq!(tester.state.messages.len(), 2);

    let message1 = tester.state.messages.pop_front().unwrap();
    assert_eq!(message1.message.content.body, b"1st");

    let message2 = tester.state.messages.pop_front().unwrap();
    assert_eq!(message2.message.content.body, b"2nd");
}

#[tokio::test]
async fn consume_unacked_removes_messages_from_the_queue_after_send() {
    let mut tester = TestCase::default().build();

    let mut frx = tester.consume("ctag-1", true).await;

    tester
        .state
        .handle_command(tester.command_publish("Message 1st"))
        .await
        .unwrap();

    recv_timeout(&mut frx).await;

    assert!(tester.state.messages.is_empty());
}

#[tokio::test]
async fn basic_get_then_basic_ack_deletes_the_message_from_the_queue() {
    let mut tester = TestCase::default().build();

    tester
        .state
        .handle_command(tester.command_publish("Acked message"))
        .await
        .unwrap();

    let mut frx = tester.passive_consume().await;

    let fr = recv_timeout(&mut frx).await.unwrap();
    let msg = parse_message(fr).unwrap();

    assert_eq!(msg.consumer_tag, "");
    assert_eq!(msg.delivery_tag, 1);
    assert!(!msg.redelivered);

    let consumer_tag = format!("{}-{}", tester.connection_id, tester.used_channel);
    let (cmd, rx) = tester.command_basic_ack(1, &consumer_tag, msg.delivery_tag);

    //dbg!(&cmd);

    tester.state.handle_command(cmd).await.unwrap();

    //dbg!(&tester.state.outbox);

    rx.await.unwrap().unwrap();

    assert!(tester.state.messages.is_empty());
    assert!(tester.state.outbox.outgoing_messages.is_empty());
}

#[tokio::test]
async fn basic_get_and_consume_without_ack_and_get_should_redeliver() {
    let mut tester = TestCase::default().build();

    tester
        .state
        .handle_command(tester.command_publish("Redelivered message"))
        .await
        .unwrap();

    let mut frx = tester.passive_consume().await;

    let _frame = recv_timeout(&mut frx).await.unwrap();

    tester
        .state
        .handle_command(tester.command_passive_cancel())
        .await
        .unwrap();

    let mut frx = tester.passive_consume().await;
    let fr = recv_timeout(&mut frx).await.unwrap();

    println!("{:?}", fr);

    let msg = parse_message(fr).unwrap();

    assert!(msg.redelivered);
}

#[tokio::test]
async fn exclusive_cannot_be_bound_to_others_exchange() {
    let mut tester = TestCase::default().build();

    let (tx, rx) = oneshot::channel();
    let exchange_bound = QueueCommand::ExchangeBound {
        conn_id: "other".to_string(),
        channel: 3u16,
        exchange_name: "valid-exchange".to_string(),
        result: tx,
    };

    tester.state.handle_command(exchange_bound).await.unwrap();

    let res = rx.await.unwrap();
    println!("{:?}", res);
    assert!(res.is_err());

    let err = to_runtime_error(res);
    assert_eq!(err.channel, 3u16);
    assert_eq!(err.scope, ErrorScope::Channel);
    assert_eq!(err.code, ChannelError::AccessRefused as u16);
}
