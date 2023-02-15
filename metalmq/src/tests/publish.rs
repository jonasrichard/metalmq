use metalmq_codec::frame::{self, BasicPublishArgs};

use crate::tests::{recv_timeout, send_content, unpack_frames, TestCase};

#[tokio::test]
async fn basic_publish_mandatory_message() {
    let tc = TestCase::new().await;
    let (mut client, mut client_rx) = tc.new_client();

    // Publish message to an exchange which doesn't route to queues -> channel error
    let publish = BasicPublishArgs::new("x-direct")
        .routing_key("invalid-key")
        .mandatory(true);

    client.basic_publish(1u16, publish).await.unwrap();

    send_content(&mut client, b"A simple message").await;

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

    send_content(&mut client, b"Another message").await;

    // No message is expected as a response
    let expected_timeout = dbg!(recv_timeout(&mut client_rx).await);
    assert!(expected_timeout.is_none());
}
