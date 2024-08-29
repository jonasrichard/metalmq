use crate::tests::{
    test_client::{unpack_frames, unpack_single_frame},
    TestCase,
};
use metalmq_codec::frame::{self, ExchangeDeclareArgs};

#[tokio::test]
async fn bind_queue_with_validation() {
    let test_case = TestCase::new().await;
    let mut test_client = test_case.new_client();

    // Normal exchange declaration sends back ExchangeDeclareOk
    let args = ExchangeDeclareArgs::default()
        .exchange_name("normal-exchange")
        .exchange_type("direct");

    test_client.exchange_declare(1, args).await;

    let exchange_declare_ok = unpack_single_frame(test_client.recv_timeout().await.unwrap());

    assert!(matches!(exchange_declare_ok, frame::AMQPFrame::Method(_, _, _)));

    // Declaring reserved exchanges ends up in channel error
    let args = ExchangeDeclareArgs::default()
        .exchange_name("amq.reserved")
        .exchange_type("direct");
    test_client.exchange_declare(2, args).await;

    let channel_error = unpack_single_frame(test_client.recv_timeout().await.unwrap());

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
    let result = test_client.exchange_declare(3, args).await;

    //let channel_error = to_runtime_error(result);

    //assert_eq!(channel_error.code, ConnectionError::CommandInvalid as u16);
}

#[tokio::test]
async fn queue_purge_clean_the_queue() {
    let test_case = TestCase::new().await;
    let mut test_client = test_case.new_client();

    for i in 0..16 {
        test_client
            .publish_content(1, "x-fanout", "", format!("Message #{i}").as_bytes())
            .await;
    }

    test_client
        .connection
        .handle_client_frame(frame::QueuePurgeArgs::default().queue_name("q-fanout").frame(2))
        .await
        .unwrap();

    let purge_ok = unpack_frames(test_client.recv_timeout().await.unwrap());
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
    let test_case = TestCase::new().await;
    let mut test_client = test_case.new_client();

    // Declare and queue and an exchange and bind them
    test_client
        .queue_declare(1, frame::QueueDeclareArgs::default().name("queue-delete-test"))
        .await;
    test_client.recv_frames().await;

    test_client
        .exchange_declare(
            1,
            frame::ExchangeDeclareArgs::default()
                .exchange_name("exchange-delete-test")
                .exchange_type("direct"),
        )
        .await;
    test_client.recv_frames().await;

    test_client
        .connection
        .handle_client_frame(
            frame::QueueBindArgs::new("queue-delete-test", "exchange-delete-test")
                .routing_key("key")
                .frame(1),
        )
        .await
        .unwrap();
    test_client.recv_frames().await;

    //let mut consumer = tc.new_client();

    //// Start to consume the queue
    //consumer
    //    .basic_consume(
    //        2,
    //        frame::BasicConsumeArgs::default()
    //            .queue("queue-delete-test")
    //            .consumer_tag("ct"),
    //    )
    //    .await
    //    .unwrap();
    //recv_frames(&mut consumer_rx).await;

    //// Delete the queue
    //client
    //    .queue_delete(1, frame::QueueDeleteArgs::default().queue_name("queue-delete-test"))
    //    .await
    //    .unwrap();
    //let queue_delete_ok = recv_frames(&mut client_rx).await;

    //assert!(matches!(
    //    dbg!(queue_delete_ok).get(0).unwrap(),
    //    frame::AMQPFrame::Method(
    //        1,
    //        _,
    //        frame::MethodFrameArgs::QueueDeleteOk(frame::QueueDeleteOkArgs { .. })
    //    )
    //));

    //let cancel_consume = recv_frames(&mut consumer_rx).await;

    //assert!(matches!(
    //    dbg!(cancel_consume).get(0).unwrap(),
    //    frame::AMQPFrame::Method(2, _, frame::MethodFrameArgs::BasicCancel(frame::BasicCancelArgs { .. }))
    //));

    // exchange should be unbound
}
