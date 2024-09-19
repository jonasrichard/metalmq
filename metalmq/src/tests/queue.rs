use crate::tests::{test_case::TestCase, test_client::sleep};
use metalmq_codec::frame::{self, ExchangeDeclareArgs};

#[tokio::test]
async fn bind_queue_with_validation() {
    let test_case = TestCase::new().await;
    let mut client = test_case.new_client_with_channels(&[1, 2, 3]).await;

    // Normal exchange declaration sends back ExchangeDeclareOk
    let args = ExchangeDeclareArgs::default()
        .exchange_name("normal-exchange")
        .exchange_type("direct");

    let exchange_declare_ok = client.exchange_declare(1, args).await;

    assert!(matches!(
        dbg!(exchange_declare_ok),
        frame::AMQPFrame::Method(1, _, frame::MethodFrameArgs::ExchangeDeclareOk)
    ));

    // TODO figure out how to pre-define exchanges in the exchange manager

    // Declaring reserved exchanges ends up in channel error
    //let args = ExchangeDeclareArgs::default()
    //    .exchange_name("amq.reserved")
    //    .exchange_type("direct");

    //let channel_closed_error = client.send_frame_with_response(args.frame(2)).await;

    //assert!(matches!(
    //    channel_closed_error,
    //    frame::AMQPFrame::Method(
    //        2u16,
    //        _,
    //        frame::MethodFrameArgs::ChannelClose(frame::ChannelCloseArgs { code: 403, .. })
    //    )
    //));

    // Declaring invalid exchange e.g. empty name ends up in connection error
    //let args = ExchangeDeclareArgs::default();

    //let result = client.send_frame_with_response(args.frame(3)).await;

    //let channel_error = to_runtime_error(result);

    //assert_eq!(channel_error.code, ConnectionError::CommandInvalid as u16);
}

#[tokio::test]
async fn queue_purge_clean_the_queue() {
    let test_case = TestCase::new().await;
    let mut client = test_case.new_client_with_channels(&[1, 2]).await;

    for i in 0..16 {
        client
            .publish_content(1, "x-fanout", "", format!("Message #{i}").as_bytes())
            .await;
    }

    sleep(100).await;

    client
        .send_frame(frame::QueuePurgeArgs::default().queue_name("q-fanout").frame(2))
        .await;

    let purge_ok = client.recv_single_frame().await;
    assert!(matches!(
        dbg!(purge_ok),
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
    let mut client = test_case.new_client_with_channel(1).await;

    // Declare and queue and an exchange and bind them
    let _ = client
        .queue_declare(1, frame::QueueDeclareArgs::default().name("queue-delete-test"))
        .await;

    let _ = client
        .exchange_declare(
            1,
            frame::ExchangeDeclareArgs::default()
                .exchange_name("exchange-delete-test")
                .exchange_type("direct"),
        )
        .await;

    let _ = client
        .send_frame(
            frame::QueueBindArgs::new("queue-delete-test", "exchange-delete-test")
                .routing_key("key")
                .frame(1),
        )
        .await;

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
