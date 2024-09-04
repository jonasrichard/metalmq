use metalmq_codec::frame::{self, AMQPFrame};

use crate::tests::TestCase;
use crate::Result;

#[tokio::test]
async fn connect_with_username_password() -> Result<()> {
    let test_case = TestCase::new().await;
    let mut client = test_case.new_client();

    client.connection.handle_client_frame(frame::AMQPFrame::Header).await?;

    let connection_start = client.recv_single_frame().await;

    assert!(matches!(
        connection_start,
        AMQPFrame::Method(
            0,
            frame::CONNECTION_START,
            frame::MethodFrameArgs::ConnectionStart(frame::ConnectionStartArgs {
                version_major: 0,
                version_minor: 9,
                ..
            })
        )
    ));

    client
        .connection
        .handle_client_frame(frame::ConnectionStartOkArgs::new("guest", "guest").frame())
        .await?;

    let connection_tune = client.recv_single_frame().await;

    dbg!(&connection_tune);

    assert!(matches!(
        connection_tune,
        AMQPFrame::Method(
            0,
            frame::CONNECTION_TUNE,
            frame::MethodFrameArgs::ConnectionTune(frame::ConnectionTuneArgs {
                channel_max: 2047,
                frame_max: 131_072,
                heartbeat: 60
            })
        )
    ));

    client
        .connection
        .handle_client_frame(
            frame::ConnectionOpenArgs {
                virtual_host: "/".into(),
                insist: false,
            }
            .frame(),
        )
        .await
        .unwrap();

    let connection_open_ok = client.recv_single_frame().await;

    assert!(matches!(
        connection_open_ok,
        AMQPFrame::Method(0, frame::CONNECTION_OPEN_OK, frame::MethodFrameArgs::ConnectionOpenOk)
    ));

    Ok(())
}

#[tokio::test]
async fn connect_and_open_channel() -> Result<()> {
    let test_case = TestCase::new().await;
    let mut client = test_case.new_connected_client(1).await;

    client
        .connection
        .handle_client_frame(frame::channel_close(1, 200, "Normal close", frame::CHANNEL_CLOSE).into())
        .await?;

    let channel_close_ok = client.recv_single_frame().await;

    assert!(matches!(
        dbg!(channel_close_ok),
        AMQPFrame::Method(1, frame::CHANNEL_CLOSE_OK, frame::MethodFrameArgs::ChannelCloseOk)
    ));

    client
        .connection
        .handle_client_frame(frame::connection_close(200, "Normal close", frame::CONNECTION_CLOSE).into())
        .await?;

    let connection_close_ok = client.recv_single_frame().await;

    assert!(matches!(
        dbg!(connection_close_ok),
        AMQPFrame::Method(_, frame::CONNECTION_CLOSE_OK, frame::MethodFrameArgs::ConnectionCloseOk)
    ));

    Ok(())
}
