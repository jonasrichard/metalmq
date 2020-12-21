use super::state::{self, Connection};
use crate::{Context, Result};
use futures::stream::StreamExt;
use futures::SinkExt;
use ironmq_codec::codec::AMQPCodec;
use ironmq_codec::frame::{self, AMQPFrame, MethodFrameArgs};
use log::{error, info};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_util::codec::Framed;

pub(crate) async fn handle_client(socket: TcpStream, context: Arc<Mutex<Context>>) -> Result<()> {
    let (mut sink, mut stream) = Framed::new(socket, AMQPCodec {}).split();
    let mut conn = state::new(context);

    while let Some(payload) = stream.next().await {
        info!("Payload {:?}", payload);

        match payload {
            Ok(frame) => match handle_client_frame(&mut *conn, frame).await? {
                Some(response_frame) => {
                    if let AMQPFrame::Method(_, frame::CONNECTION_CLOSE_OK, _) = response_frame {
                        info!("Outgoing {:?}", response_frame);
                        sink.send(response_frame).await?;

                        return Ok(());
                    } else {
                        info!("Outgoing {:?}", response_frame);
                        sink.send(response_frame).await?;
                    }
                }
                None => (),
            },
            Err(e) => return Err(Box::new(e)),
        }
    }

    Ok(())
}

//type SinkType = SplitSink<Framed<TcpStream, AMQPCodec>, AMQPFrame>;

async fn handle_client_frame(conn: &mut dyn Connection, f: AMQPFrame) -> Result<Option<AMQPFrame>> {
    use AMQPFrame::*;

    match f {
        Header => Ok(Some(frame::connection_start(0))),
        Method(ch, _, mf) => handle_method_frame(conn, ch, mf).await,
        ContentHeader(ch) => conn.receive_content_header(ch).await,
        ContentBody(cb) => conn.receive_content_body(cb).await,
        _ => {
            error!("Unhandler frame type {:?}", f);
            Ok(None)
        }
    }
}

async fn handle_method_frame(conn: &mut dyn Connection, channel: frame::Channel,
                             ma: frame::MethodFrameArgs) -> Result<Option<AMQPFrame>> {
    use MethodFrameArgs::*;

    match ma {
        ConnectionStartOk(_) => Ok(Some(frame::connection_tune(0))),
        ConnectionTuneOk(_) => Ok(None),
        ConnectionOpen(args) => conn.connection_open(channel, args).await,
        ConnectionClose(args) => conn.connection_close(args).await,
        ChannelOpen => conn.channel_open(channel).await,
        ChannelClose(args) => conn.channel_close(channel, args).await,
        ExchangeDeclare(args) => conn.exchange_declare(channel, args).await,
        QueueDeclare(args) => conn.queue_declare(channel, args).await,
        QueueBind(args) => conn.queue_bind(channel, args).await,
        BasicPublish(args) => conn.basic_publish(channel, args).await,
        _ => {
            error!("Unhandler method frame type {:?}", ma);
            Ok(None)
        }
    }
}
