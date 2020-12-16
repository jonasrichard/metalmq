use crate::conn_state;
use crate::conn_state::Connection;
use crate::{Context, Result};
use futures::stream::StreamExt;
use futures::SinkExt;
use ironmq_codec::codec::AMQPCodec;
use ironmq_codec::frame;
use ironmq_codec::frame::{AMQPFrame, MethodFrameArgs};
use log::{error, info};
use std::sync::{Arc, Mutex};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

pub(crate) async fn handle_client(socket: TcpStream, context: Arc<Mutex<Context>>) -> Result<()> {
    let (mut sink, mut stream) = Framed::new(socket, AMQPCodec{}).split();
    let mut cs = conn_state::new(context);

    while let Some(payload) = stream.next().await {
        info!("Payload {:?}", payload);

        match payload {
            Ok(frame) => match handle_client_frame(&mut cs, frame).await? {
                Some(response_frame) => {
                    if let AMQPFrame::Method(_, frame::CONNECTION_CLOSE_OK, _) = response_frame {
                        sink.send(response_frame).await?;

                        return Ok(())
                    } else {
                        sink.send(response_frame).await?;
                    }
                },
                None => (),
            },
            Err(e) => return Err(Box::new(e)),
        }
    }

    Ok(())
}

//type SinkType = SplitSink<Framed<TcpStream, AMQPCodec>, AMQPFrame>;

async fn handle_client_frame(
    mut cs: &mut Connection,
    f: AMQPFrame,
) -> Result<Option<AMQPFrame>> {
    match f {
        AMQPFrame::Header => Ok(Some(frame::connection_start(0))),
        AMQPFrame::Method(ch, _, mf) => handle_method_frame(cs, ch, mf).await,
        AMQPFrame::ContentHeader(ch) => conn_state::receive_content_header(&mut cs, ch).await,
        AMQPFrame::ContentBody(cb) => conn_state::receive_content_body(&mut cs, cb).await,
        _ => {
            error!("Unhandler frame type {:?}", f);
            Ok(None)
        },
    }
}

async fn handle_method_frame(
    mut cs: &mut Connection,
    channel: frame::Channel,
    ma: frame::MethodFrameArgs,
) -> Result<Option<AMQPFrame>> {
    match ma {
        MethodFrameArgs::ConnectionStartOk(_) => Ok(Some(frame::connection_tune(0))),
        MethodFrameArgs::ConnectionTuneOk(_) => Ok(None),
        MethodFrameArgs::ConnectionOpen(args) => conn_state::connection_open(&mut cs, channel, args).await,
        MethodFrameArgs::ConnectionClose(args) => conn_state::connection_close(&mut cs, args).await,
        MethodFrameArgs::ChannelOpen => conn_state::channel_open(&mut cs, channel).await,
        MethodFrameArgs::ChannelClose(args) => conn_state::channel_close(&mut cs, channel, args).await,
        MethodFrameArgs::ExchangeDeclare(args) => conn_state::exchange_declare(&mut cs, channel, args).await,
        MethodFrameArgs::QueueDeclare(args) => conn_state::queue_declare(&mut cs, channel, args).await,
        MethodFrameArgs::QueueBind(args) => conn_state::queue_bind(&mut cs, channel, args).await,
        MethodFrameArgs::BasicPublish(args) => conn_state::basic_publish(&mut cs, channel, args).await,
        _ => {
            error!("Unhandler method frame type {:?}", ma);
            Ok(None)
        },
    }
}
