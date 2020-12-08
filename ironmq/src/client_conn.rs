use crate::conn_state;
use crate::conn_state::ConnectionState;
use crate::Result;
use futures::stream::{SplitSink, StreamExt};
use futures::SinkExt;
use ironmq_codec::codec::AMQPCodec;
use ironmq_codec::frame;
use ironmq_codec::frame::{AMQPFrame, MethodFrame};
use log::{error, info};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;

pub(crate) async fn handle_client(socket: TcpStream) -> Result<()> {
    let (mut sink, mut stream) = Framed::new(socket, AMQPCodec {}).split();
    let mut cs = ConnectionState {};

    // TODO create client connection struct

    while let Some(payload) = stream.next().await {
        info!("Payload {:?}", payload);

        match payload {
            Ok(frame) => match handle_client_frame(&mut cs, frame).await? {
                Some(response_frame) => sink.send(response_frame).await?,
                None => (),
            },
            Err(e) => return Err(Box::new(e)),
        }
    }

    Ok(())
}

//type SinkType = SplitSink<Framed<TcpStream, AMQPCodec>, AMQPFrame>;

async fn handle_client_frame(
    mut cs: &mut ConnectionState,
    f: AMQPFrame,
) -> Result<Option<AMQPFrame>> {
    match f {
        AMQPFrame::AMQPHeader => Ok(Some(wrap(frame::connection_start(0)))),
        AMQPFrame::Method(mf) => handle_method_frame(&mut cs, *mf).await,
        _ => Ok(None),
    }
}

async fn handle_method_frame(
    mut cs: &mut ConnectionState,
    mf: frame::MethodFrame,
) -> Result<Option<AMQPFrame>> {
    let response = match mf.class_method {
        frame::CONNECTION_START_OK => Ok(Some(frame::connection_tune(0))),
        frame::CONNECTION_TUNE_OK => Ok(None),
        frame::CONNECTION_OPEN => client_conn::connection_open(&mut cs, mf),
        _ => Ok(None),
    };

    response.map(|o| o.map(|mf| AMQPFrame::Method(Box::new(mf))))
}

fn wrap(mf: MethodFrame) -> AMQPFrame {
    AMQPFrame::Method(Box::new(mf))
}
