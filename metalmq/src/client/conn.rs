use super::state::{self, Connection, FrameResponse};
use crate::{Context, Result};
use futures::stream::StreamExt;
use futures::SinkExt;
use log::{error, trace};
use metalmq_codec::codec::AMQPCodec;
use metalmq_codec::frame::{self, AMQPFrame, MethodFrameArgs};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex};
use tokio_util::codec::Framed;

pub(crate) async fn handle_client(socket: TcpStream, context: Arc<Mutex<Context>>) -> Result<()> {
    let (mut sink, mut stream) = Framed::new(socket, AMQPCodec {}).split();
    let (consume_sink, mut consume_stream) = mpsc::channel::<AMQPFrame>(1);
    let mut conn = state::new(context, consume_sink);

    loop {
        tokio::select! {
            data = stream.next() => {
                trace!("Payload {:?}", data);

                match data {
                    Some(payload) =>
                        match payload {
                            Ok(frame) => match handle_client_frame(&mut conn, frame).await? {
                                FrameResponse::Frame(response_frame) => {
                                    if let AMQPFrame::Method(_, frame::CONNECTION_CLOSE_OK, _) = response_frame {
                                        trace!("Outgoing {:?}", response_frame);
                                        sink.send(response_frame).await?;

                                        return Ok(());
                                    } else {
                                        trace!("Outgoing {:?}", response_frame);
                                        sink.send(response_frame).await?;
                                    }
                                },
                                FrameResponse::Frames(response_frames) => {
                                    for f in response_frames {
                                        trace!("Outgoing {:?}", f);
                                        sink.send(f).await?;
                                    }
                                },
                                FrameResponse::None => {
                                    conn.connection_close(frame::ConnectionCloseArgs::default()).await?;
                                    ()
                                },
                            },
                            Err(e) => return Err(Box::new(e)),
                        },
                    None => break Ok(())
                }
            }
            push = consume_stream.recv() => {
                match push {
                    Some(outgoing) =>
                        sink.send(outgoing).await?,
                    None =>
                        ()  // TODO is it closed?
                }
            }
        }
    }
}

//type SinkType = SplitSink<Framed<TcpStream, AMQPCodec>, AMQPFrame>;

async fn handle_client_frame(conn: &mut Connection, f: AMQPFrame) -> Result<FrameResponse> {
    use AMQPFrame::*;

    match f {
        Header => Ok(FrameResponse::Frame(frame::connection_start(0))),
        Method(ch, _, mf) => handle_method_frame(conn, ch, mf).await,
        ContentHeader(ch) => conn.receive_content_header(ch).await,
        ContentBody(cb) => conn.receive_content_body(cb).await,
        Heartbeat(_) => Ok(FrameResponse::None),
        //_ => {
        //    error!("Unhandler frame type {:?}", f);
        //    Ok(None)
        //}
    }
}

async fn handle_method_frame(
    conn: &mut Connection,
    channel: frame::Channel,
    ma: frame::MethodFrameArgs,
) -> Result<FrameResponse> {
    use MethodFrameArgs::*;

    match ma {
        ConnectionStartOk(_) => Ok(FrameResponse::Frame(frame::connection_tune(0))),
        ConnectionTuneOk(_) => Ok(FrameResponse::None),
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
        ConfirmSelect(args) => conn.confirm_select(channel, args).await,
        _ => {
            error!("Unhandler method frame type {:?}", ma);
            Ok(FrameResponse::None)
        }
    }
}
