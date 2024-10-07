//! Handles the TCP socket level connection.
//!
//! When a client connects a new tokio process is spawned which handles the incoming frames. The
//! frames are already decoded by [`AMQPCodec`]. To avoid blocking the outgoing frames are passed
//! by a different `mpsc::channel` usually named as `outgoing`. In that way we avoided the circular
//! blocking of channel senders and receivers.
//!
//! Incoming and outgoing loop handles the heartbeats which we expect and send in a timely manner.
//!
//! The [`handle_in_stream_data`] function forwards the frames to the connection.
use crate::{
    client::connection::types::Connection,
    error::{to_runtime_error, Result},
    Context,
};
use futures::{
    stream::{SplitSink, SplitStream, StreamExt},
    SinkExt,
};
use log::{error, trace, warn};
use metalmq_codec::{
    codec::{AMQPCodec, Frame},
    frame,
};
use std::time::Duration;
use tokio::{net::TcpStream, sync::mpsc};
use tokio_util::codec::Framed;

/// Accept the client connection and spawn tokio threads to handle outgoing message sending.
pub(crate) async fn handle_client(socket: TcpStream, context: Context) -> Result<()> {
    let (sink, stream) = Framed::new(socket, AMQPCodec {}).split();
    // We use channels with only one item long, so we can block until the frame is sent out.
    let (consume_sink, mut consume_stream) = mpsc::channel::<Frame>(16);
    let mut conn = Connection::new(context, consume_sink);
    let heartbeat_duration = conn.heartbeat_interval;

    tokio::spawn(async move {
        if let Err(e) = if let Some(heartbeat) = heartbeat_duration {
            outgoing_loop_with_heartbeat(sink, &mut consume_stream, heartbeat).await
        } else {
            outgoing_loop(sink, &mut consume_stream).await
        } {
            // FIXME here we can catch an 'Os { code: 32, kind: BrokenPipe, message: "Broken pipe"
            // }' and we need to clean up the connection data, channels, queues, etc
            error!("Error {e:?}");
        }
    });

    if let Some(heartbeat) = heartbeat_duration {
        incoming_loop_with_heartbeat(&mut conn, stream, heartbeat).await?
    } else {
        incoming_loop(&mut conn, stream).await?
    }

    conn.close().await?;

    Ok(())
}

async fn incoming_loop(conn: &mut Connection, mut stream: SplitStream<Framed<TcpStream, AMQPCodec>>) -> Result<()> {
    while let Some(data) = stream.next().await {
        trace!("Incoming {data:?}");

        if let Err(e) = data {
            error!("Error during reading stream {:?}", e);
            //conn.handle_error(to_runtime_error(Box::new(e))).await?;

            break;
        }

        let data = data?;
        match handle_in_stream_data(conn, data).await {
            Ok(false) => break,
            Ok(true) => (),
            Err(e) => {
                error!("This should have been handled {:?}", e);
                break;
                //conn.handle_error(to_runtime_error(e)).await?;
            }
        }
    }

    // TODO this is not true if connection is suddenly closed
    //debug_assert!(conn.channel_receivers.is_empty());
    //debug_assert!(conn.channel_handlers.is_empty());

    Ok(())
}

async fn incoming_loop_with_heartbeat(
    conn: &mut Connection,
    mut stream: SplitStream<Framed<TcpStream, AMQPCodec>>,
    heartbeat_duration: Duration,
) -> Result<()> {
    let heartbeat = tokio::time::interval(heartbeat_duration);
    tokio::pin!(heartbeat);

    // Time instant of the last message received.
    //
    // Server has an interval which ticks in every heartbeat duration. If it hasn't received any
    // messages longer than two heartbeat time, it raises a connection exception.
    let mut last_message_received = tokio::time::Instant::now();

    'input: loop {
        tokio::select! {
            input_data = stream.next() => {
                match input_data {
                    Some(data) => {
                        trace!("Incoming {data:?}");

                        last_message_received = tokio::time::Instant::now();

                        if let Err(e) = data {
                            // EXPLAINATION this is handled by the Connection.handle_client_frame
                            //conn.handle_error(to_runtime_error(Box::new(e))).await?;

                            break 'input Ok(());
                        }

                        // In case of normal closing of the stream we suppose that cleanup has been
                        // already done.
                        let data = data?;
                        match handle_in_stream_data(conn, data).await {
                            Ok(false) => break 'input Ok(()),
                            Ok(true) => (),
                            Err(e) => {
                                // EXPLAINATION this is handled by the Connection.handle_client_frame
                                // TODO what is this???
                                //conn.handle_error(to_runtime_error(e)).await?;
                            }
                        }
                    }
                    None => {
                        break 'input Ok(());
                    }
                }
            }
            _ = heartbeat.tick() => {
                if last_message_received.elapsed() > 2 * heartbeat_duration {
                    warn!("No heartbeat arrived for {:?}, closing connection", last_message_received.elapsed());

                    // If server misses a heartbeat it needs to close the connection without the
                    // connection close/close-ok handshake.
                    //
                    // TODO but still we need to close consumer resources and auto delete queues,
                    // exchanges.
                    break 'input Ok(());
                }
            }
        }
    }
}

/// Handle sending of outgoing frames. The underlying futures sink flushes the sent items, so
/// when we get back the control it means that the frames have already been sent.
async fn outgoing_loop(
    mut socket_sink: SplitSink<Framed<TcpStream, AMQPCodec>, Frame>,
    frame_stream: &mut mpsc::Receiver<Frame>,
) -> Result<()> {
    while let Some(frame) = frame_stream.recv().await {
        trace!("Outgoing {frame:?}");

        socket_sink.send(frame).await?;
    }

    Ok(())
}

async fn outgoing_loop_with_heartbeat(
    mut socket_sink: SplitSink<Framed<TcpStream, AMQPCodec>, Frame>,
    frame_stream: &mut mpsc::Receiver<Frame>,
    heartbeat: Duration,
) -> Result<()> {
    // While sending out frames, server has a timer which ticks in every half heartbeat time. If
    // there is no frame sent out for a bit more than half tick time, server sends a heartbeat out.
    let half_heartbeat = heartbeat.div_f32(2.0);
    let heartbeat = tokio::time::interval(half_heartbeat);
    tokio::pin!(heartbeat);

    // Time instant of the last message sent.
    let mut last_message_sent = tokio::time::Instant::now();

    loop {
        tokio::select! {
            maybe_out_frame = frame_stream.recv() => {
                match maybe_out_frame {
                    Some(frame) => {
                        trace!("Outgoing {frame:?}");

                        socket_sink.send(frame).await?;
                    }
                    None => {
                        break;
                    }
                }
            }
            _ = heartbeat.tick() => {
                if last_message_sent.elapsed() > half_heartbeat {
                    last_message_sent = tokio::time::Instant::now();

                    socket_sink.send(Frame::Frame(frame::heartbeat())).await?;
                }
            }
        }
    }

    Ok(())
}

/// Handles the incoming frame by the connection or connection routes to the appropriate channel.
/// If it returns `Ok(false)` that means that the loop should be closed.
pub async fn handle_in_stream_data(conn: &mut Connection, data: Frame) -> Result<bool> {
    match data {
        Frame::Frame(frame) => {
            dbg!(&frame);

            if !conn.handle_client_frame(frame).await? {
                return Ok(false);
            }

            Ok(true)
        }
        Frame::Frames(frames) => {
            for frame in frames {
                if !conn.handle_client_frame(frame).await? {
                    return Ok(false);
                }
            }

            Ok(true)
        }
    }
}
