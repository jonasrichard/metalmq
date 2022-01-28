use crate::client::state::{self, Connection};
use crate::{Context, Result};
use futures::stream::{SplitSink, StreamExt};
use futures::SinkExt;
use log::{error, trace};
use metalmq_codec::codec::{AMQPCodec, Frame};
use metalmq_codec::frame::{self, AMQPFrame, MethodFrameArgs};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::codec::Framed;

pub(crate) async fn handle_client(socket: TcpStream, context: Context) -> Result<()> {
    let (sink, mut stream) = Framed::new(socket, AMQPCodec {}).split();
    // We use channels with only one item long, so we can block until the frame is sent out.
    let (consume_sink, mut consume_stream) = mpsc::channel::<Frame>(1);
    let mut conn = state::new(context, consume_sink);

    tokio::spawn(async move {
        if let Err(e) = outgoing_loop(sink, &mut consume_stream).await {
            error!("Error {:?}", e);
        }
    });

    // TODO
    // We need to start monitor heartbeat messages after connection open.
    //
    // Probably conn needs to have a field like last message arrived. If we haven't received
    // anything for a longer period, we can close the connection.
    //
    // Connection.Tune decides how frequently we send and expect the heartbeat frames.
    //let half_heartbeat = conn.get_half_heartbeat();
    let half_heartbeat = std::time::Duration::from_secs(5);
    let heartbeat = tokio::time::interval(half_heartbeat);
    tokio::pin!(heartbeat);

    // Time instant of the last message received or the last heartbeat is sent.
    // If more time than the half heartbeat interval passed since the last message received, we can
    // send a heartbeat.
    //
    // Note: we are sending twice as much heartbeat than necessary, but at least
    // we are handling timer intervals in a resource conscious way.
    let mut last_message_received = tokio::time::Instant::now();

    'input: loop {
        tokio::select! {
            input_data = stream.next() => {
                match input_data {
                    Some(data) => {
                        trace!("Incoming {:?}", data);

                        last_message_received = tokio::time::Instant::now();

                        if !handle_in_stream_data(&mut conn, data).await? {
                            break 'input;
                        }
                    }
                    None => {
                        break 'input;
                    }
                }
            }
            _ = heartbeat.tick() => {
                trace!("Heartbeat ticked, last time {:?}", last_message_received);

                if last_message_received.elapsed() > half_heartbeat {
                    last_message_received = tokio::time::Instant::now();

                    conn.send_heartbeat().await?;
                }
            }
        }
    }

    conn.cleanup().await?;

    Ok(())
}

/// Handle sending of outgoing frames. The underlying futures sink flushes the sent items, so
/// when we get back the control it means that the frames have already been sent.
async fn outgoing_loop(
    mut socket_sink: SplitSink<Framed<TcpStream, AMQPCodec>, Frame>,
    frame_stream: &mut mpsc::Receiver<Frame>,
) -> Result<()> {
    while let Some(frame) = frame_stream.recv().await {
        trace!("Outgoing {:?}", frame);

        socket_sink.send(frame).await?;
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
        Header => conn.send_frame(Frame::Frame(frame::connection_start(0))).await,
        Method(ch, _, mf) => handle_method_frame(conn, ch, mf).await,
        ContentHeader(ch) => conn.receive_content_header(ch).await,
        ContentBody(cb) => conn.receive_content_body(cb).await,
        // TODO heartbeat frames must have channel 0, if we get an invalid frame we need to raise
        // connection expection 501 - invalid frame.
        Heartbeat(_) => Ok(()),
    }
}

async fn handle_method_frame(conn: &mut Connection, channel: frame::Channel, ma: frame::MethodFrameArgs) -> Result<()> {
    use MethodFrameArgs::*;

    match ma {
        ConnectionStartOk(args) => conn.connection_start_ok(channel, args).await,
        ConnectionTuneOk(_) => Ok(()),
        ConnectionOpen(args) => conn.connection_open(channel, args).await,
        ConnectionClose(args) => conn.connection_close(args).await,
        ChannelOpen => conn.channel_open(channel).await,
        ChannelClose(args) => conn.channel_close(channel, args).await,
        ChannelCloseOk => conn.channel_close_ok(channel).await,
        ExchangeDeclare(args) => conn.exchange_declare(channel, args).await,
        ExchangeDelete(args) => conn.exchange_delete(channel, args).await,
        QueueDeclare(args) => conn.queue_declare(channel, args).await,
        QueueBind(args) => conn.queue_bind(channel, args).await,
        QueueDelete(args) => conn.queue_delete(channel, args).await,
        QueueUnbind(args) => conn.queue_unbind(channel, args).await,
        BasicPublish(args) => conn.basic_publish(channel, args).await,
        BasicConsume(args) => conn.basic_consume(channel, args).await,
        BasicCancel(args) => conn.basic_cancel(channel, args).await,
        BasicAck(args) => conn.basic_ack(channel, args).await,
        ConfirmSelect(args) => conn.confirm_select(channel, args).await,
        _ => {
            error!("Unhandler method frame type {:?}", ma);
            Ok(())
        }
    }
}
