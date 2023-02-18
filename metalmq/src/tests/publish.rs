use metalmq_codec::frame::{self, BasicPublishArgs};

use crate::tests::{recv_timeout, send_content, sleep, unpack_frames, TestCase};

#[tokio::test]
async fn basic_publish_mandatory_message() {
    let tc = TestCase::new().await;
    let (mut client, mut client_rx) = tc.new_client();

    // Publish message to an exchange which doesn't route to queues -> channel error
    let publish = BasicPublishArgs::new("x-direct")
        .routing_key("invalid-key")
        .mandatory(true);

    client.basic_publish(1u16, publish).await.unwrap();

    send_content(&mut client, 1u16, b"A simple message").await;

    // Since the routing key is not matching and message is mandatory, server sends back the message
    // with a Basic.Return frame
    let return_frames = unpack_frames(recv_timeout(&mut client_rx).await.unwrap());

    assert!(matches!(
        dbg!(return_frames).get(0).unwrap(),
        frame::AMQPFrame::Method(1u16, _, frame::MethodFrameArgs::BasicReturn(_))
    ));

    client
        .basic_publish(
            1u16,
            BasicPublishArgs::new("x-direct")
                .routing_key("magic-key")
                .mandatory(true),
        )
        .await
        .unwrap();

    send_content(&mut client, 1u16, b"Another message").await;

    // No message is expected as a response
    let expected_timeout = dbg!(recv_timeout(&mut client_rx).await);
    assert!(expected_timeout.is_none());
}

#[tokio::test]
async fn basic_get_empty_and_ok() {
    let tc = TestCase::new().await;
    let (mut client, mut client_rx) = tc.new_client();

    client
        .basic_get(2, frame::BasicGetArgs::new("q-fanout").no_ack(false))
        .await
        .unwrap();

    let get_empty_frames = unpack_frames(recv_timeout(&mut client_rx).await.unwrap());

    assert!(matches!(
        dbg!(get_empty_frames).get(0).unwrap(),
        frame::AMQPFrame::Method(2, _, frame::MethodFrameArgs::BasicGetEmpty)
    ));

    client
        .basic_publish(1, frame::BasicPublishArgs::new("x-fanout"))
        .await
        .unwrap();
    send_content(&mut client, 1u16, b"A fanout message").await;

    sleep(100).await;

    client.basic_get(2, frame::BasicGetArgs::new("q-fanout")).await.unwrap();

    let get_frames = unpack_frames(recv_timeout(&mut client_rx).await.unwrap());

    assert!(matches!(
        dbg!(get_frames).get(0).unwrap(),
        frame::AMQPFrame::Method(
            2,
            _,
            frame::MethodFrameArgs::BasicGetOk(frame::BasicGetOkArgs { redelivered: false, .. })
        )
    ));
}

#[tokio::test]
async fn basic_ack_multiple() {
    let tc = TestCase::new().await;
    let (mut client, mut client_rx) = tc.new_client();

    // Send 10 messages
    for i in 0..10 {
        client
            .basic_publish(
                3,
                dbg!(frame::BasicPublishArgs::new("x-direct").routing_key("magic-key")),
            )
            .await
            .unwrap();
        send_content(&mut client, 3u16, format!("Ack test #{i}").as_bytes()).await;
    }

    sleep(100).await;

    // Consume 10 messages with another client
    let (mut consumer, mut consumer_rx) = tc.new_client();

    consumer
        .basic_consume(
            4,
            frame::BasicConsumeArgs::default()
                .queue("q-direct")
                .consumer_tag("unit-test"),
        )
        .await
        .unwrap();

    let consumer_ok = unpack_frames(recv_timeout(&mut consumer_rx).await.unwrap());
    assert!(matches!(
        dbg!(consumer_ok).get(0).unwrap(),
        frame::AMQPFrame::Method(4, _, frame::MethodFrameArgs::BasicConsumeOk(_))
    ));

    let mut last_delivery_tag = 0u64;

    for _ in 0..10 {
        let message_frames = unpack_frames(recv_timeout(&mut consumer_rx).await.unwrap());

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
        .await
        .unwrap();

    // Check if queue is empty by deleting and if it is empty
    client
        .queue_delete(
            4,
            frame::QueueDeleteArgs::default().queue_name("q-direct").if_empty(true),
        )
        .await
        .unwrap();

    let delete_ok = unpack_frames(recv_timeout(&mut client_rx).await.unwrap());
    assert!(matches!(
        dbg!(delete_ok).get(0).unwrap(),
        frame::AMQPFrame::Method(
            4,
            _,
            frame::MethodFrameArgs::QueueDeleteOk(frame::QueueDeleteOkArgs { message_count: 0 })
        )
    ));
}
