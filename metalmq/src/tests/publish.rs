use metalmq_codec::frame::{self, BasicPublishArgs};

use crate::tests::{
    test_client::{basic_deliver_args, sleep, unpack_frames},
    TestCase,
};

#[tokio::test]
async fn basic_publish_mandatory_message() {
    let test_case = TestCase::new().await;
    let mut test_client = test_case.new_client_with_channel(1).await;

    // Publish message to an exchange which doesn't route to queues -> channel error
    let mandatory_message = frame::BasicPublishArgs::new("x-direct")
        .routing_key("invalid-key")
        .mandatory(true)
        .frame(1);

    test_client.send_frame(mandatory_message).await;
    test_client.send_content(1, b"An unroutable message").await;

    // Since the routing key is not matching and message is mandatory, server sends back the message
    // with a Basic.Return frame
    let return_frames = test_client.recv_frames().await;

    assert!(matches!(
        dbg!(return_frames).first().unwrap(),
        frame::AMQPFrame::Method(1u16, _, frame::MethodFrameArgs::BasicReturn(_))
    ));

    test_client
        .basic_publish(
            1u16,
            BasicPublishArgs::new("x-direct")
                .routing_key("magic-key")
                .mandatory(true),
        )
        .await;

    test_client.send_content(1u16, b"Another message").await;

    // No message is expected as a response
    let expected_timeout = dbg!(test_client.recv_timeout().await);
    assert!(expected_timeout.is_none());

    test_client.close_channel(1).await;
    test_client.close().await;
}

#[tokio::test]
async fn basic_get_empty_and_ok() {
    let test_case = TestCase::new().await;
    let mut test_client = test_case.new_client_with_channels(&[1, 2]).await;

    test_client
        .basic_get(2, frame::BasicGetArgs::new("q-fanout").no_ack(false))
        .await;

    let get_empty_frames = test_client.recv_frames().await;

    assert!(matches!(
        dbg!(get_empty_frames).get(0).unwrap(),
        frame::AMQPFrame::Method(2, _, frame::MethodFrameArgs::BasicGetEmpty)
    ));

    test_client
        .basic_publish(1, frame::BasicPublishArgs::new("x-fanout"))
        .await;
    test_client.send_content(1u16, b"A fanout message").await;

    sleep(100).await;

    test_client.basic_get(2, frame::BasicGetArgs::new("q-fanout")).await;

    let get_frames = test_client.recv_frames().await;

    assert!(matches!(
        dbg!(get_frames).first().unwrap(),
        frame::AMQPFrame::Method(
            2,
            _,
            frame::MethodFrameArgs::BasicGetOk(frame::BasicGetOkArgs { redelivered: false, .. })
        )
    ));

    test_case.teardown().await;
}

#[tokio::test]
async fn basic_ack_multiple() {
    let test_case = TestCase::new().await;
    let mut test_client = test_case.new_client_with_channel(3).await;

    // Send 10 messages
    for i in 0..10 {
        test_client
            .basic_publish(
                3,
                dbg!(frame::BasicPublishArgs::new("x-direct").routing_key("magic-key")),
            )
            .await;
        test_client
            .send_content(3u16, format!("Ack test #{i}").as_bytes())
            .await;
    }

    sleep(100).await;

    // Consume 10 messages with another client
    let mut consumer = test_case.new_client_with_channel(4).await;

    consumer
        .basic_consume(
            4,
            frame::BasicConsumeArgs::default()
                .queue("q-direct")
                .consumer_tag("unit-test"),
        )
        .await;

    let consumer_ok = consumer.recv_single_frame().await;
    assert!(matches!(
        dbg!(consumer_ok),
        frame::AMQPFrame::Method(4, _, frame::MethodFrameArgs::BasicConsumeOk(_))
    ));

    let mut last_delivery_tag = 0u64;

    for _ in 0..10 {
        let message_frames = consumer.recv_frames().await;

        dbg!(&message_frames);

        // Catch the delivery tag from the Basic.Deliver
        if let frame::AMQPFrame::Method(
            _,
            _,
            frame::MethodFrameArgs::BasicDeliver(frame::BasicDeliverArgs { delivery_tag, .. }),
        ) = message_frames.get(0).unwrap()
        {
            last_delivery_tag = *delivery_tag;
        }
    }

    // Multi ack the consuming with the last delivery tag
    consumer
        .basic_ack(
            4,
            frame::BasicAckArgs::default()
                .delivery_tag(last_delivery_tag)
                .multiple(true),
        )
        .await;

    consumer.basic_cancel(4, frame::BasicCancelArgs::new("unit-test")).await;

    let cancel_ok = consumer.recv_single_frame().await;

    assert!(matches!(
        cancel_ok,
        frame::AMQPFrame::Method(4, _, frame::MethodFrameArgs::BasicCancelOk(_))
    ));

    // Check if queue is empty by deleting and if it is empty
    consumer
        .queue_delete(
            4,
            frame::QueueDeleteArgs::default().queue_name("q-direct").if_empty(true),
        )
        .await;

    let delete_ok = consumer.recv_single_frame().await;
    assert!(matches!(
        dbg!(delete_ok),
        frame::AMQPFrame::Method(
            4,
            _,
            frame::MethodFrameArgs::QueueDeleteOk(frame::QueueDeleteOkArgs { message_count: 0 })
        )
    ));
}

#[tokio::test]
async fn publish_to_topic_exchange() {
    let test_case = TestCase::new().await;
    let mut test_client = test_case.new_client_with_channels(&[1, 2]).await;

    test_client.open_channel(2).await;

    test_client
        .publish_content(1, "x-topic", "topic.key", b"Topic test")
        .await;

    test_client
        .basic_consume(
            2,
            frame::BasicConsumeArgs::default().queue("q-topic").consumer_tag("ctag"),
        )
        .await;

    let _consume_ok = unpack_frames(test_client.recv_timeout().await.unwrap());

    let delivery = unpack_frames(test_client.recv_timeout().await.unwrap());
    let mut frames = delivery.into_iter();

    let basic_deliver = basic_deliver_args(frames.next().unwrap());
    assert_eq!("x-topic", basic_deliver.exchange_name);
    // ...

    if let frame::AMQPFrame::ContentHeader(header) = frames.next().unwrap() {
        assert_eq!(2, header.channel);
    }

    if let frame::AMQPFrame::ContentBody(body) = frames.next().unwrap() {
        assert_eq!(b"Topic test", body.body.as_slice());
    }

    test_case.teardown().await;
}
