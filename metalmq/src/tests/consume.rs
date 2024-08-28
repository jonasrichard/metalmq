use metalmq_codec::frame::{self, AMQPFrame::Method, BasicCancelArgs, BasicConsumeArgs, MethodFrameArgs};

use crate::tests::*;

#[tokio::test]
async fn one_consumer() {
    let test_case = TestCase::new().await;
    let mut test_client = test_case.new_client();

    test_client
        .basic_consume(1, BasicConsumeArgs::default().queue("q-direct").consumer_tag("ctag"))
        .await;

    let consume_ok = recv_frames(&mut test_client.conn_rx).await;

    assert!(matches!(
        dbg!(consume_ok.get(0)).unwrap(),
        frame::AMQPFrame::Method(
            1,
            _,
            frame::MethodFrameArgs::BasicConsumeOk(frame::BasicConsumeOkArgs { .. })
        )
    ));

    test_client
        .publish_content(2, "x-direct", "magic-key", b"Consume test")
        .await;

    let mut deliver = recv_frames(&mut test_client.conn_rx).await;
    assert_eq!(dbg!(&deliver).len(), 3);

    let deliver_args = basic_deliver_args(deliver.remove(0));

    assert_eq!(deliver_args.consumer_tag, "ctag");
    assert_eq!(deliver_args.delivery_tag, 1u64);
    assert!(!deliver_args.redelivered);

    test_client.basic_cancel(1, BasicCancelArgs::new("ctag")).await;

    let cancel_ok = recv_single_frame(&mut test_client.conn_rx).await;

    let args = if let Method(_, _, MethodFrameArgs::BasicCancelOk(args)) = cancel_ok {
        args
    } else {
        panic!("Not a Basic.CancelOk frame");
    };

    assert_eq!(args.consumer_tag, "ctag");

    test_case.teardown().await;
}

#[tokio::test]
async fn one_consumer_redeliver() {
    let test_case = TestCase::new().await;
    let mut test_client = test_case.new_client();

    // Consume the queue
    test_client
        .basic_consume(1, BasicConsumeArgs::default().queue("q-direct").consumer_tag("ctag"))
        .await;
    recv_frames(&mut test_client.conn_rx).await;

    // Publish a message
    test_client
        .publish_content(2, "x-direct", "magic-key", b"Redeliver test")
        .await;

    // Receive it via basic deliver
    let mut deliver = recv_frames(&mut test_client.conn_rx).await;
    let _deliver_args = basic_deliver_args(deliver.remove(0));

    // Cancel consumer
    test_client.basic_cancel(1, BasicCancelArgs::new("ctag")).await;
    let _cancel_ok = recv_single_frame(&mut test_client.conn_rx).await;

    // Consume the queue again
    test_client
        .basic_consume(3, BasicConsumeArgs::default().queue("q-direct").consumer_tag("ctag2"))
        .await;
    recv_frames(&mut test_client.conn_rx).await;

    // Receive the message again
    let mut deliver = recv_frames(&mut test_client.conn_rx).await;
    let deliver_args = basic_deliver_args(deliver.remove(0));

    assert_eq!(deliver_args.consumer_tag, "ctag2");
    assert_eq!(deliver_args.delivery_tag, 1u64);
    assert!(deliver_args.redelivered);

    test_case.teardown().await;
}
