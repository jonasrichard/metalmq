use metalmq_codec::frame::{
    self, AMQPFrame::Method, BasicCancelArgs, BasicConsumeArgs, BasicPublishArgs, MethodFrameArgs,
};

use crate::tests::{basic_deliver_args, recv_frames, recv_single_frame, send_content, TestCase};

#[tokio::test]
async fn one_consumer() {
    let tc = TestCase::new().await;
    let (mut client, mut client_rx) = tc.new_client();

    client
        .basic_consume(1, BasicConsumeArgs::default().queue("q-direct").consumer_tag("ctag"))
        .await
        .unwrap();

    let consume_ok = recv_frames(&mut client_rx).await;

    assert!(matches!(
        dbg!(consume_ok.get(0)).unwrap(),
        frame::AMQPFrame::Method(
            1,
            _,
            frame::MethodFrameArgs::BasicConsumeOk(frame::BasicConsumeOkArgs { .. })
        )
    ));

    client
        .basic_publish(2, BasicPublishArgs::new("x-direct").routing_key("magic-key"))
        .await
        .unwrap();
    send_content(&mut client, 2, b"Consume test").await;

    let mut deliver = recv_frames(&mut client_rx).await;
    assert_eq!(dbg!(&deliver).len(), 3);

    let deliver_args = basic_deliver_args(deliver.remove(0));

    assert_eq!(deliver_args.consumer_tag, "ctag");
    assert_eq!(deliver_args.delivery_tag, 1u64);
    assert_eq!(deliver_args.redelivered, false);

    client.basic_cancel(1, BasicCancelArgs::new("ctag")).await.unwrap();

    let cancel_ok = recv_single_frame(&mut client_rx).await;

    let args = if let Method(_, _, MethodFrameArgs::BasicCancelOk(args)) = cancel_ok {
        args
    } else {
        panic!("Not a Basic.CancelOk frame");
    };

    assert_eq!(args.consumer_tag, "ctag");
}
