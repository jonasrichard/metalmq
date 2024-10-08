use metalmq_codec::frame::{self, AMQPFrame};

use crate::error::{ErrorScope, Result, RuntimeError};
use crate::tests::{recv, test_case::TestCase};

/// Frame level test of a connection start, tune and open.
#[tokio::test]
async fn connect_with_username_password() -> Result<()> {
    let test_case = TestCase::new().await;
    let mut client = test_case.new_client().await;

    client.send_frame(frame::AMQPFrame::Header).await;

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
        .send_frame(frame::ConnectionStartOkArgs::new("guest", "guest").frame())
        .await;

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
        .send_frame(
            frame::ConnectionOpenArgs {
                virtual_host: "/".into(),
                insist: false,
            }
            .frame(),
        )
        .await;

    let connection_open_ok = client.recv_single_frame().await;

    assert!(matches!(
        connection_open_ok,
        AMQPFrame::Method(0, frame::CONNECTION_OPEN_OK, frame::MethodFrameArgs::ConnectionOpenOk)
    ));

    Ok(())
}

/// Test a connection open, channel open, channel close and connection close positive flow.
#[tokio::test]
async fn connect_and_open_channel() -> Result<()> {
    let test_case = TestCase::new().await;
    let mut client = test_case.new_client_with_channel(1).await;

    client
        .send_frame(frame::channel_close(1, 200, "Normal close", frame::CHANNEL_CLOSE).into())
        .await;

    let channel_close_ok = client.recv_single_frame().await;

    assert!(matches!(
        dbg!(channel_close_ok),
        AMQPFrame::Method(1, frame::CHANNEL_CLOSE_OK, frame::MethodFrameArgs::ChannelCloseOk)
    ));

    client
        .send_frame(frame::connection_close(200, "Normal close", frame::CONNECTION_CLOSE).into())
        .await;

    let connection_close_ok = client.recv_single_frame().await;

    assert!(matches!(
        dbg!(connection_close_ok),
        AMQPFrame::Method(_, frame::CONNECTION_CLOSE_OK, frame::MethodFrameArgs::ConnectionCloseOk)
    ));

    assert!(client.connection.await.is_ok());

    Ok(())
}

/// Connect with bad password should end up in connection error.
#[tokio::test]
async fn connect_with_bad_password() -> Result<()> {
    let test_case = TestCase::new().await;
    let mut client = test_case.new_client().await;

    let _connection_start = client.send_frame_with_response(frame::AMQPFrame::Header).await;

    let connection_error = client
        .send_frame_with_response(frame::ConnectionStartOkArgs::new("guest", "badpassword").frame())
        .await;

    assert!(matches!(
        dbg!(connection_error),
        AMQPFrame::Method(0, _, frame::MethodFrameArgs::ConnectionClose(_))
    ));

    assert!(client.connection.await.is_ok());

    Ok(())
}

/// Opening two channels with the same number should end up in connection error.
#[tokio::test]
async fn channel_reopen_with_same_number() -> Result<()> {
    let test_case = TestCase::new().await;
    let mut client = test_case.new_client_with_channel(1).await;

    client.send_frame(frame::channel_open(1)).await;
    client.send_frame(frame::channel_open(1)).await;

    let connection_error = recv::recv_error_frame(&mut client.conn_rx).await;

    assert!(matches!(
        connection_error,
        RuntimeError {
            scope: ErrorScope::Connection,
            ..
        },
    ));

    assert!(dbg!(client.connection.await).is_ok());

    Ok(())
}
