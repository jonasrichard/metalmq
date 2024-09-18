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

    debug_assert!(conn.channel_receivers.is_empty());
    debug_assert!(conn.channel_handlers.is_empty());

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

// FIXME new design is needed
//
// In the new design each channel should be a spawned thread, because the goal of the channels is
// that the client and the server can send messages parallelly.
//
// So all of these fn calls will be message sendings with an return value mpsc channel here. When
// we send out a message to a channel, we already have that return value channel cloned in the
// channel. So we can listen the messages coming back and we can react on that. The return value
// channel is only good for handling the errors because the reponse AMQP frames will be sent out
// via the cloned outgoing channel.

// TODO this will be in the new Connection impl
//async fn handle_method_frame(
//    conn: &mut Connection,
//    channel: frame::Channel,
//    cm: u32,
//    ma: frame::MethodFrameArgs,
//) -> Result<()> {
//    use MethodFrameArgs::*;
//
//    let r = match ma {
//        ConnectionStartOk(args) => conn.connection_start_ok(channel, args).await,
//        ConnectionTuneOk(args) => conn.connection_tune_ok(channel, args).await,
//        ConnectionOpen(args) => conn.connection_open(channel, args).await,
//        ConnectionClose(args) => conn.connection_close(args).await,
//        ChannelOpen => conn.channel_open(channel).await,
//        ChannelClose(args) => conn.channel_close(channel, args).await,
//        ChannelCloseOk => conn.channel_close_ok(channel).await,
//        ExchangeDeclare(args) => conn.exchange_declare(channel, args).await,
//        ExchangeDelete(args) => conn.exchange_delete(channel, args).await,
//        QueueDeclare(args) => conn.queue_declare(channel, args).await,
//        QueueBind(args) => conn.queue_bind(channel, args).await,
//        QueuePurge(args) => conn.queue_purge(channel, args).await,
//        QueueDelete(args) => conn.queue_delete(channel, args).await,
//        QueueUnbind(args) => conn.queue_unbind(channel, args).await,
//        BasicPublish(args) => conn.basic_publish(channel, args).await,
//        BasicConsume(args) => conn.basic_consume(channel, args).await,
//        BasicCancel(args) => conn.basic_cancel(channel, args).await,
//        BasicAck(args) => conn.basic_ack(channel, args).await,
//        BasicGet(args) => conn.basic_get(channel, args).await,
//        BasicReject(args) => conn.basic_reject(channel, args).await,
//        ConfirmSelect(args) => conn.confirm_select(channel, args).await,
//        _ => {
//            error!("Unhandler method frame type {ma:?}");
//            Ok(())
//        }
//    };
//
//    if r.is_err() {
//        trace!("Method frame handled with error {:?}", r);
//    }
//
//    r
//}
