use crate::client;
use crate::client::state::{self, Connection};
use crate::{Context, Result};
use futures::stream::{SplitSink, SplitStream, StreamExt};
use futures::SinkExt;
use log::{error, trace, warn};
use metalmq_codec::codec::{AMQPCodec, Frame};
use metalmq_codec::frame::{self, AMQPFrame, MethodFrameArgs};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::codec::Framed;

pub(crate) async fn handle_client(socket: TcpStream, context: Context) -> Result<()> {
    let (sink, stream) = Framed::new(socket, AMQPCodec {}).split();
    // We use channels with only one item long, so we can block until the frame is sent out.
    let (consume_sink, mut consume_stream) = mpsc::channel::<Frame>(1);
    let mut conn = state::new(context, consume_sink);
    let heartbeat_duration = conn.get_heartbeat();

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

    conn.cleanup().await?;

    Ok(())
}

async fn incoming_loop(conn: &mut Connection, mut stream: SplitStream<Framed<TcpStream, AMQPCodec>>) -> Result<()> {
    while let Some(data) = stream.next().await {
        trace!("Incoming {data:?}");

        if !handle_in_stream_data(conn, data).await? {
            break;
        }
    }

    Ok(())
}

async fn incoming_loop_with_heartbeat(
    conn: &mut Connection,
    mut stream: SplitStream<Framed<TcpStream, AMQPCodec>>,
    heartbeat_duration: Duration,
) -> Result<()> {
    // TODO
    // We need to start monitor heartbeat messages after connection open.
    //
    // Probably conn needs to have a field like last message arrived. If we haven't received
    // anything for a longer period, we can close the connection.
    //
    // Connection.Tune decides how frequently we send and expect the heartbeat frames.
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

                        if !handle_in_stream_data(conn, data).await? {
                            break 'input;
                        }
                    }
                    None => {
                        break 'input;
                    }
                }
            }
            _ = heartbeat.tick() => {
                if last_message_received.elapsed() > 2 * heartbeat_duration {
                    warn!("No heartbeat arrived for {:?}, closing connection", last_message_received.elapsed());

                    // TODO should we raise a connection exception instead?
                    break 'input;
                }
            }
        }
    }

    Ok(())
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

async fn handle_in_stream_data(
    conn: &mut Connection,
    data: std::result::Result<Frame, std::io::Error>,
) -> Result<bool> {
    match data {
        Ok(Frame::Frame(frame)) => {
            if handle_client_frame(conn, frame).await.is_err() {
                conn.cleanup().await?;

                return Ok(false);
            }

            Ok(true)
        }
        Ok(Frame::Frames(frames)) => {
            for frame in frames {
                if handle_client_frame(conn, frame).await.is_err() {
                    conn.cleanup().await?;

                    return Ok(false);
                }
            }

            Ok(true)
        }
        Err(e) => Err(Box::new(e)),
    }

    // TODO here we need to do the cleanup
}

async fn handle_client_frame(conn: &mut Connection, f: AMQPFrame) -> Result<()> {
    use AMQPFrame::*;

    match f {
        Header => {
            conn.send_frame(Frame::Frame(frame::ConnectionStartArgs::new().frame()))
                .await
        }
        Method(ch, _, mf) => handle_method_frame(conn, ch, mf).await,
        ContentHeader(ch) => conn.receive_content_header(ch).await,
        ContentBody(cb) => conn.receive_content_body(cb).await,
        Heartbeat(0) => Ok(()),
        Heartbeat(_) => {
            client::connection_error(0, client::ConnectionError::FrameError, "Heartbeat must have channel 0")
        }
    }
}

async fn handle_method_frame(conn: &mut Connection, channel: frame::Channel, ma: frame::MethodFrameArgs) -> Result<()> {
    use MethodFrameArgs::*;

    match ma {
        ConnectionStartOk(args) => conn.connection_start_ok(channel, args).await,
        ConnectionTuneOk(args) => conn.connection_tune_ok(channel, args).await,
        ConnectionOpen(args) => conn.connection_open(channel, args).await,
        ConnectionClose(args) => conn.connection_close(args).await,
        ChannelOpen => conn.channel_open(channel).await,
        ChannelClose(args) => conn.channel_close(channel, args).await,
        ChannelCloseOk => conn.channel_close_ok(channel).await,
        ExchangeDeclare(args) => conn.exchange_declare(channel, args).await,
        ExchangeDelete(args) => conn.exchange_delete(channel, args).await,
        QueueDeclare(args) => conn.queue_declare(channel, args).await,
        QueueBind(args) => conn.queue_bind(channel, args).await,
        QueuePurge(args) => conn.queue_purge(channel, args).await,
        QueueDelete(args) => conn.queue_delete(channel, args).await,
        QueueUnbind(args) => conn.queue_unbind(channel, args).await,
        BasicPublish(args) => conn.basic_publish(channel, args).await,
        BasicConsume(args) => conn.basic_consume(channel, args).await,
        BasicCancel(args) => conn.basic_cancel(channel, args).await,
        BasicAck(args) => conn.basic_ack(channel, args).await,
        BasicGet(args) => conn.basic_get(channel, args).await,
        BasicReject(args) => conn.basic_reject(channel, args).await,
        ConfirmSelect(args) => conn.confirm_select(channel, args).await,
        _ => {
            error!("Unhandler method frame type {ma:?}");
            Ok(())
        }
    }
}
