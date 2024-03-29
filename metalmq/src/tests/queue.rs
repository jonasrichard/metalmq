use crate::{
    client::{tests::to_runtime_error, ConnectionError},
    tests::{recv_frames, recv_timeout, send_content, unpack_frames, unpack_single_frame, TestCase},
};
use metalmq_codec::frame::{self, ExchangeDeclareArgs};

#[tokio::test]
async fn bind_queue_with_validation() {
    let tc = TestCase::new().await;
    let (mut client, mut client_rx) = tc.new_client();

    // Normal exchange declaration sends back ExchangeDeclareOk
    let args = ExchangeDeclareArgs::default()
        .exchange_name("normal-exchange")
        .exchange_type("direct");
    client.exchange_declare(1u16, args).await.unwrap();

    let exchange_declare_ok = unpack_single_frame(recv_timeout(&mut client_rx).await.unwrap());

    assert!(matches!(exchange_declare_ok, frame::AMQPFrame::Method(_, _, _)));

    // Declaring reserved exchanges ends up in channel error
    let args = ExchangeDeclareArgs::default()
        .exchange_name("amq.reserved")
        .exchange_type("direct");
    client.exchange_declare(2u16, args).await.unwrap();

    let channel_error = unpack_single_frame(recv_timeout(&mut client_rx).await.unwrap());

    assert!(matches!(
        channel_error,
        frame::AMQPFrame::Method(
            2u16,
            _,
            frame::MethodFrameArgs::ChannelClose(frame::ChannelCloseArgs { code: 403, .. })
        )
    ));

    // Declaring invalid exchange e.g. empty name ends up in connection error
    let args = ExchangeDeclareArgs::default();
    let result = client.exchange_declare(3u16, args).await;

    let channel_error = to_runtime_error(result);

    assert_eq!(channel_error.code, ConnectionError::CommandInvalid as u16);
}

#[tokio::test]
async fn queue_purge_clean_the_queue() {
    let tc = TestCase::new().await;
    let (mut client, mut client_rx) = tc.new_client();

    for i in 0..16 {
        client
            .basic_publish(1, frame::BasicPublishArgs::new("x-fanout"))
            .await
            .unwrap();
        send_content(&mut client, 1u16, format!("Message #{i}").as_bytes()).await;
    }

    client
        .queue_purge(2, frame::QueuePurgeArgs::default().queue_name("q-fanout"))
        .await
        .unwrap();

    let purge_ok = unpack_frames(recv_timeout(&mut client_rx).await.unwrap());
    assert!(matches!(
        dbg!(purge_ok).get(0).unwrap(),
        frame::AMQPFrame::Method(
            _,
            _,
            frame::MethodFrameArgs::QueuePurgeOk(frame::QueuePurgeOkArgs { message_count: 16 })
        )
    ));
}

#[tokio::test]
async fn queue_delete_unbind_and_cancel_consume() {
    let tc = TestCase::new().await;
    let (mut client, mut client_rx) = tc.new_client();

    // Declare and queue and an exchange and bind them
    client
        .queue_declare(1, frame::QueueDeclareArgs::default().name("queue-delete-test"))
        .await
        .unwrap();
    recv_frames(&mut client_rx).await;

    client
        .exchange_declare(
            1,
            frame::ExchangeDeclareArgs::default()
                .exchange_name("exchange-delete-test")
                .exchange_type("direct"),
        )
        .await
        .unwrap();
    recv_frames(&mut client_rx).await;

    client
        .queue_bind(
            1,
            frame::QueueBindArgs::new("queue-delete-test", "exchange-delete-test").routing_key("key"),
        )
        .await
        .unwrap();
    recv_frames(&mut client_rx).await;

    let (mut consumer, mut consumer_rx) = tc.new_client();

    // Start to consume the queue
    consumer
        .basic_consume(
            2,
            frame::BasicConsumeArgs::default()
                .queue("queue-delete-test")
                .consumer_tag("ct"),
        )
        .await
        .unwrap();
    recv_frames(&mut consumer_rx).await;

    // Delete the queue
    client
        .queue_delete(1, frame::QueueDeleteArgs::default().queue_name("queue-delete-test"))
        .await
        .unwrap();
    let queue_delete_ok = recv_frames(&mut client_rx).await;

    assert!(matches!(
        dbg!(queue_delete_ok).get(0).unwrap(),
        frame::AMQPFrame::Method(
            1,
            _,
            frame::MethodFrameArgs::QueueDeleteOk(frame::QueueDeleteOkArgs { .. })
        )
    ));

    let cancel_consume = recv_frames(&mut consumer_rx).await;

    assert!(matches!(
        dbg!(cancel_consume).get(0).unwrap(),
        frame::AMQPFrame::Method(2, _, frame::MethodFrameArgs::BasicCancel(frame::BasicCancelArgs { .. }))
    ));

    // exchange should be unbound
}
