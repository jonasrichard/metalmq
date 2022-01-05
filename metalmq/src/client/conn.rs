use crate::client::state::{self, Connection};
use crate::{Context, Result};
use futures::stream::{SplitSink, StreamExt};
use futures::SinkExt;
use log::{error, trace};
use metalmq_codec::codec::{AMQPCodec, Frame};
use metalmq_codec::frame::{self, AMQPFrame, MethodFrameArgs};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::Framed;

#[derive(Debug)]
pub enum SendFrame {
    Async(Frame),
    Sync(Frame, oneshot::Sender<bool>),
}

pub(crate) async fn handle_client(socket: TcpStream, context: Context) -> Result<()> {
    let (sink, mut stream) = Framed::new(socket, AMQPCodec {}).split();
    // We use channels with only one item long, so we can block until the frame is sent out.
    let (mut consume_sink, mut consume_stream) = mpsc::channel::<Frame>(1);
    let mut conn = state::new(context, consume_sink.clone());

    // FIXME support feedback of sending out frames. Sometimes we need to wait for the sream to
    // fully send out frames and only after that we can execute logic. For example we need to wait
    // for sending out basic-consume-ok before sending basic-deliver. Or we need to wait for
    // sending out basic-publish and tell the caller if publish was successful. This later is
    // rather client logic but we need to support this anyway.
    tokio::spawn(async move {
        if let Err(e) = outgoing_loop(sink, &mut consume_stream).await {
            error!("Error {:?}", e);
        }
    });

    while let Some(data) = stream.next().await {
        trace!("Incoming {:?}", data);

        if !handle_in_stream_data(&mut conn, &mut consume_sink, data).await? {
            return Ok(());
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
    sink: &mut mpsc::Sender<Frame>,
    data: std::result::Result<Frame, std::io::Error>,
) -> Result<bool> {
    match data {
        Ok(Frame::Frame(frame)) => {
            if handle_client_frame(conn, *frame).await.is_err() {
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

/// Send out response frames and returns false if the connection needs to be closed
/// because the reponse contains a connection close ok frame.
async fn send_out_frame_response(sink: &mut mpsc::Sender<SendFrame>, response: Frame) -> Result<bool> {
    match response {
        Frame::Frame(frame) => {
            let f = Frame::Frame(frame);
            let closed = has_connection_close_ok(&f);

            //trace!("Outgoing {:?}", f);

            sink.send(SendFrame::Async(f)).await?;
            Ok(!closed)
        }
        Frame::Frames(frames) => {
            let fs = Frame::Frames(frames);
            let closed = has_connection_close_ok(&fs);

            //trace!("Outgoing {:?}", fs);

            sink.send(SendFrame::Async(fs)).await?;
            Ok(!closed)
        }
    }
}

fn has_connection_close_ok(frame: &Frame) -> bool {
    match frame {
        Frame::Frame(f) => is_connection_close_ok(f),
        Frame::Frames(fs) => fs.iter().any(is_connection_close_ok),
    }
}

fn is_connection_close_ok(frame: &AMQPFrame) -> bool {
    matches!(frame, &AMQPFrame::Method(_, frame::CONNECTION_CLOSE_OK, _))
}

async fn handle_client_frame(conn: &mut Connection, f: AMQPFrame) -> Result<()> {
    use AMQPFrame::*;

    match f {
        Header => {
            conn.send_frame(Frame::Frame(Box::new(frame::connection_start(0))))
                .await
        }
        Method(ch, _, mf) => handle_method_frame(conn, ch, mf).await,
        ContentHeader(ch) => conn.receive_content_header(ch).await,
        ContentBody(cb) => conn.receive_content_body(cb).await,
        Heartbeat(_) => Ok(()),
    }

    // Convert runtime error to AMQP frame
    //match result {
    //    Err(e) => match e.downcast::<RuntimeError>() {
    //        Ok(conn_err) => Ok(Some(Frame::Frame((*conn_err).into()))),
    //        Err(e2) => Err(e2),
    //    },
    //    _ => result,
    //}
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
