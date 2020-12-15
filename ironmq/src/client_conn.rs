use crate::conn_state::{Connection, ConnectionState};
use crate::Result;
use futures::stream::StreamExt;
use futures::SinkExt;
use ironmq_codec::codec::AMQPCodec;
use ironmq_codec::frame;
use ironmq_codec::frame::{AMQPFrame, MethodFrameArgs};
use log::{error, info};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

pub(crate) async fn handle_client(socket: TcpStream) -> Result<()> {
    let (mut sink, mut stream) = Framed::new(socket, AMQPCodec {}).split();
    let mut cs = ConnectionState::default();

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
    cs: &mut ConnectionState,
    f: AMQPFrame,
) -> Result<Option<AMQPFrame>> {
    match f {
        AMQPFrame::Header => Ok(Some(frame::connection_start(0))),
        AMQPFrame::Method(ch, _, mf) => handle_method_frame(cs, ch, mf).await,
        AMQPFrame::ContentHeader(ch) => cs.receive_content_header(ch),
        AMQPFrame::ContentBody(cb) => cs.receive_content_body(cb),
        _ => {
            error!("Unhandler frame type {:?}", f);
            Ok(None)
        },
    }
}

async fn handle_method_frame(
    cs: &mut ConnectionState,
    channel: frame::Channel,
    ma: frame::MethodFrameArgs,
) -> Result<Option<AMQPFrame>> {
    match ma {
        MethodFrameArgs::ConnectionStartOk(_) => Ok(Some(frame::connection_tune(0))),
        MethodFrameArgs::ConnectionTuneOk(_) => Ok(None),
        MethodFrameArgs::ConnectionOpen(args) => cs.connection_open(channel, args),
        MethodFrameArgs::ConnectionClose(args) => cs.connection_close(args),
        MethodFrameArgs::ChannelOpen => cs.channel_open(channel),
        MethodFrameArgs::ChannelClose(args) => cs.channel_close(channel, args),
        MethodFrameArgs::ExchangeDeclare(args) => cs.exchange_declare(channel, args),
        MethodFrameArgs::QueueDeclare(args) => cs.queue_declare(channel, args),
        MethodFrameArgs::QueueBind(args) => cs.queue_bind(channel, args),
        MethodFrameArgs::BasicPublish(args) => cs.basic_publish(channel, args),
        _ => {
            error!("Unhandler method frame type {:?}", ma);
            Ok(None)
        },
    }
}
