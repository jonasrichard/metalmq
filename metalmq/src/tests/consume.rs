use metalmq_codec::frame::{self, BasicCancelArgs, BasicConsumeArgs};

use crate::tests::{test_case::TestCase, test_client::basic_deliver_args};

#[tokio::test]
async fn one_consumer() {
    let test_case = TestCase::new().await;
    let mut test_client = test_case.new_client_with_channel(1).await;

    let consume_ok = test_client
        .basic_consume(1, BasicConsumeArgs::default().queue("q-direct").consumer_tag("ctag"))
        .await;

    assert!(matches!(
        dbg!(consume_ok),
        frame::AMQPFrame::Method(
            1,
            _,
            frame::MethodFrameArgs::BasicConsumeOk(frame::BasicConsumeOkArgs { .. })
        )
    ));

    test_client.open_channel(2).await;

    test_client
        .publish_content(2, "x-direct", "magic-key", b"Consume test")
        .await;

    let mut deliver = test_client.recv_frames().await;
    assert_eq!(dbg!(&deliver).len(), 3);

    let deliver_args = basic_deliver_args(deliver.remove(0));

    assert_eq!(deliver_args.consumer_tag, "ctag");
    assert_eq!(deliver_args.delivery_tag, 1u64);
    assert!(!deliver_args.redelivered);

    let cancel_ok = test_client.basic_cancel(1, BasicCancelArgs::new("ctag")).await;

    assert!(matches!(
        cancel_ok,
        frame::AMQPFrame::Method(
            1,
            _,
            frame::MethodFrameArgs::BasicCancelOk(frame::BasicCancelOkArgs { .. })
        )
    ));

    test_case.teardown().await;
}

#[tokio::test]
async fn one_consumer_redeliver() {
    let test_case = TestCase::new().await;
    let mut test_client = test_case.new_client_with_channels(&[1, 2, 3]).await;

    // Consume the queue
    let _consume_ok = test_client
        .basic_consume(1, BasicConsumeArgs::default().queue("q-direct").consumer_tag("ctag"))
        .await;

    // Publish a message
    test_client
        .publish_content(2, "x-direct", "magic-key", b"Redeliver test")
        .await;

    // Receive it via basic deliver
    let mut deliver = test_client.recv_frames().await;
    let _deliver_args = basic_deliver_args(deliver.remove(0));

    // Cancel consumer
    let _cancel_ok = test_client.basic_cancel(1, BasicCancelArgs::new("ctag")).await;

    // Consume the queue again
    let _ = test_client
        .basic_consume(3, BasicConsumeArgs::default().queue("q-direct").consumer_tag("ctag2"))
        .await;

    // Receive the message again
    let mut deliver = test_client.recv_frames().await;
    let deliver_args = basic_deliver_args(deliver.remove(0));

    assert_eq!(deliver_args.consumer_tag, "ctag2");
    assert_eq!(deliver_args.delivery_tag, 1u64);
    assert!(deliver_args.redelivered);

    test_case.teardown().await;
}
