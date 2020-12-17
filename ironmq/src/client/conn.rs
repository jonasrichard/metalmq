use super::state;
use super::state::Connection;
use crate::{Context, Result};
use futures::stream::StreamExt;
use futures::SinkExt;
use ironmq_codec::codec::AMQPCodec;
use ironmq_codec::frame;
use ironmq_codec::frame::{AMQPFrame, MethodFrameArgs};
use log::{error, info};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_util::codec::Framed;

pub(crate) async fn handle_client(socket: TcpStream, context: Arc<Mutex<Context>>) -> Result<()> {
    let (mut sink, mut stream) = Framed::new(socket, AMQPCodec {}).split();
    let mut cs = state::new(context);

    while let Some(payload) = stream.next().await {
        info!("Payload {:?}", payload);

        match payload {
            Ok(frame) => match handle_client_frame(&mut cs, frame).await? {
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

async fn handle_client_frame(mut cs: &mut Connection, f: AMQPFrame) -> Result<Option<AMQPFrame>> {
    use AMQPFrame::*;

    match f {
        Header => Ok(Some(frame::connection_start(0))),
        Method(ch, _, mf) => handle_method_frame(cs, ch, mf).await,
        ContentHeader(ch) => state::receive_content_header(&mut cs, ch).await,
        ContentBody(cb) => state::receive_content_body(&mut cs, cb).await,
        _ => {
            error!("Unhandler frame type {:?}", f);
            Ok(None)
        }
    }
}

async fn handle_method_frame(
    mut cs: &mut Connection,
    channel: frame::Channel,
    ma: frame::MethodFrameArgs,
) -> Result<Option<AMQPFrame>> {
    use MethodFrameArgs::*;

    match ma {
        ConnectionStartOk(_) => Ok(Some(frame::connection_tune(0))),
        ConnectionTuneOk(_) => Ok(None),
        ConnectionOpen(args) => state::connection_open(&mut cs, channel, args).await,
        ConnectionClose(args) => state::connection_close(&mut cs, args).await,
        ChannelOpen => state::channel_open(&mut cs, channel).await,
        ChannelClose(args) => state::channel_close(&mut cs, channel, args).await,
        ExchangeDeclare(args) => state::exchange_declare(&mut cs, channel, args).await,
        QueueDeclare(args) => state::queue_declare(&mut cs, channel, args).await,
        QueueBind(args) => state::queue_bind(&mut cs, channel, args).await,
        BasicPublish(args) => state::basic_publish(&mut cs, channel, args).await,
        _ => {
            error!("Unhandler method frame type {:?}", ma);
            Ok(None)
        }
    }
}
