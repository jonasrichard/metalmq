use super::state::{self, Connection, MaybeFrame};
use crate::{Context, Result, RuntimeError};
use futures::stream::{SplitSink, StreamExt};
use futures::SinkExt;
use log::{error, trace};
use metalmq_codec::codec::{AMQPCodec, Frame};
use metalmq_codec::frame::{self, AMQPFrame, MethodFrameArgs};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::codec::Framed;

pub(crate) async fn handle_client(socket: TcpStream, context: Context) -> Result<()> {
    let (mut sink, mut stream) = Framed::new(socket, AMQPCodec {}).split();
    let (consume_sink, mut consume_stream) = mpsc::channel::<AMQPFrame>(1);
    let mut conn = state::new(context, consume_sink);

    loop {
        tokio::select! {
            data = stream.next() => {
                trace!("Incoming {:?}", data);

                if !handle_in_stream_data(&mut conn, &mut sink, data).await? {
                    sink.close().await?;

                    return Ok(())
                }
            }
            push = consume_stream.recv() => {
                match push {
                    Some(outgoing) =>
                        sink.send(Frame::Frame(outgoing)).await?,
                    None =>
                        ()  // TODO is it closed? what to do?
                }
            }
        }
    }
}

async fn handle_in_stream_data(
    mut conn: &mut Connection,
    mut sink: &mut SplitSink<Framed<TcpStream, AMQPCodec>, Frame>,
    data: Option<std::result::Result<Frame, std::io::Error>>,
) -> Result<bool> {
    match data {
        Some(Ok(Frame::Frame(frame))) => {
            match handle_client_frame(&mut conn, frame).await? {
                Some(response) => {
                    let closed = !send_out_frame_response(&mut sink, response).await?;

                    if closed {
                        // TODO cleanup

                        return Ok(false);
                    }

                    Ok(true)
                }
                None => Ok(true),
            }
        }
        Some(Ok(Frame::Frames(frames))) => {
            for frame in frames {
                match handle_client_frame(&mut conn, frame).await? {
                    Some(response) => {
                        let closed = !send_out_frame_response(&mut sink, response).await?;

                        if closed {
                            // TODO cleanup

                            return Ok(false);
                        }
                    }
                    None => (),
                }
            }

            Ok(true)
        }
        Some(Err(e)) => Err(Box::new(e)),
        None => Ok(false),
    }
}

/// Send out response frames and returns false if the connection needs to be closed
/// because the reponse contains a connection close ok frame.
async fn send_out_frame_response(
    sink: &mut SplitSink<Framed<TcpStream, AMQPCodec>, Frame>,
    response: Frame,
) -> Result<bool> {
    match response {
        Frame::Frame(frame) => {
            trace!("Outgoing {:?}", frame);

            let f = Frame::Frame(frame);
            let closed = has_connection_close_ok(&f);

            sink.send(f).await?;
            Ok(!closed)
        }
        Frame::Frames(frames) => {
            trace!("Outgoing {:?}", frames);

            let fs = Frame::Frames(frames);
            let closed = has_connection_close_ok(&fs);

            sink.send(fs).await?;
            Ok(!closed)
        }
    }
}

fn has_connection_close_ok(frame: &Frame) -> bool {
    match frame {
        Frame::Frame(f) => is_connection_close_ok(f),
        Frame::Frames(fs) => fs.iter().position(|f| is_connection_close_ok(&f)).is_some(),
    }
}

fn is_connection_close_ok(frame: &AMQPFrame) -> bool {
    match frame {
        &AMQPFrame::Method(_, frame::CONNECTION_CLOSE_OK, _) => true,
        _ => false,
    }
}

async fn handle_client_frame(conn: &mut Connection, f: AMQPFrame) -> MaybeFrame {
    use AMQPFrame::*;

    let result = match f {
        Header => Ok(Some(Frame::Frame(frame::connection_start(0)))),
        Method(ch, _, mf) => handle_method_frame(conn, ch, mf).await,
        ContentHeader(ch) => conn.receive_content_header(ch).await,
        ContentBody(cb) => conn.receive_content_body(cb).await,
        Heartbeat(_) => Ok(None),
    };

    // Convert runtime error to AMQP frame
    match result {
        Err(e) => match e.downcast::<RuntimeError>() {
            Ok(conn_err) => Ok(Some(Frame::Frame((*conn_err).into()))),
            Err(e2) => Err(e2),
        },
        _ => result,
    }
}

async fn handle_method_frame(conn: &mut Connection, channel: frame::Channel, ma: frame::MethodFrameArgs) -> MaybeFrame {
    use MethodFrameArgs::*;

    match ma {
        ConnectionStartOk(args) => conn.connection_start_ok(channel, args).await,
        ConnectionTuneOk(_) => Ok(None),
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
            Ok(None)
        }
    }
}
