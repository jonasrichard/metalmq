use std::time::Duration;

use crate::{
    client::{tests::to_runtime_error, ConnectionError},
    tests::{recv_timeout, send_content, unpack_frames, unpack_single_frame, TestCase},
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
    send_content(&mut client, b"A fanout message").await;

    tokio::time::sleep(Duration::from_millis(100)).await;

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
async fn queue_purge_clean_the_queue() {
    let tc = TestCase::new().await;
    let (mut client, mut client_rx) = tc.new_client();

    for i in 0..16 {
        client
            .basic_publish(1, frame::BasicPublishArgs::new("x-fanout"))
            .await
            .unwrap();
        send_content(&mut client, &format!("Message #{i}").as_bytes()).await;
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
