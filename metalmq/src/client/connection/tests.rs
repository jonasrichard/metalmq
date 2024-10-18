// Test cases
//   - here we need to test if specific events made the necessary state changes in Connection
//   struct
//
// connection tune with good and bad numbers
//
// connection open with bad virtual host
//   - probably I have more tests when there will be multiple virtual hosts from config
//
// connection close closes all the channels

use metalmq_codec::frame;
use tokio::sync::mpsc;

use crate::{exchange, queue, tests::recv, Context};

use super::types::Connection;

fn new_context() -> Context {
    let em = exchange::manager::start();
    let qm = queue::manager::start(em.clone());

    Context {
        exchange_manager: em,
        queue_manager: qm,
    }
}

#[tokio::test]
async fn connection_close_clean_up_channels() {
    use metalmq_codec::frame::{AMQPFrame, MethodFrameArgs};

    let ctx = new_context();
    let (tx, mut rx) = mpsc::channel(1);
    let mut connection = Connection::new(ctx, tx);

    connection.handle_client_frame(frame::AMQPFrame::Header).await.unwrap();

    let connection_start = recv::recv_single_frame(&mut rx).await;

    assert!(matches!(
        connection_start,
        AMQPFrame::Method(0, _, MethodFrameArgs::ConnectionStart(_))
    ));

    connection
        .handle_connection_start_ok(frame::ConnectionStartOkArgs::new("guest", "guest"))
        .await
        .unwrap();

    let _connection_tune = recv::recv_single_frame(&mut rx).await;

    connection
        .handle_connection_tune_ok(frame::ConnectionTuneOkArgs::default())
        .await
        .unwrap();

    connection
        .handle_connection_open(frame::ConnectionOpenArgs::default().virtual_host("/"))
        .await
        .unwrap();

    let _connection_open_ok = recv::recv_single_frame(&mut rx).await;

    // start a channel
    // start a consumer
    //
    // test should cancel consumer, close channel, stop channel spawned task, and close connection

    connection.handle_channel_open(11).await.unwrap();

    let _channel_open_ok = recv::recv_single_frame(&mut rx).await;

    assert!(connection.channel_receivers.contains_key(&11));

    connection
        .handle_connection_close(frame::ConnectionCloseArgs::default())
        .await
        .unwrap();

    //assert!(!connection.channel_handlers.contains_key(&11));

    // TODO this is a deadlock situation, here we need to have a blocking call - probably we don't
    // need - which emits channel close and other messages, but it will get channel close ok
    // message from client, then it can go and close the other channel or other resources, and so
    // on.
    // The problem however is that we block the incoming loop of the connection and if a channel
    // close ok message arrives, we cannot route to the appropriate channel. So maybe connection
    // close should be an implicit event, we need to
    //   - if we are asked to close the connection, we need to check if there are channels open
    //     - if not we can send back the connection close ok message and we are done
    //     - if not we need to start closing all the channels when all the channel close ok
    //     messages coming back we can close the connection. But what if the client doesn't send
    //     back all the close messages? We will leak connections... okay we have heartbeat, so it
    //     will close connection, but...

    let _channel_close = recv::recv_single_frame(&mut rx).await;

    connection.handle_channel_close_ok(11).await.unwrap();

    //let connection_close_ok = recv::recv_single_frame(&mut rx).await;
}
