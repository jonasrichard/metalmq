use crate::codec::{AMQPCodec, AMQPFieldValue, AMQPFrame, AMQPValue};
use crate::frame;
use crate::Result;
use futures::SinkExt;
use futures::stream::{StreamExt};
use log::{info, error};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::codec::{Framed};

pub struct Connection {
    sender_channel: mpsc::Sender<AMQPFrame>,
}

struct ChannelState {
}

pub trait Channel {
    fn basic_publish(&self, data: [u8]);
}

pub async fn connect(url: String) -> Result<Box<Connection>> {
    match TcpStream::connect(url).await {
        Ok(socket) => {
            let (sender, receiver) = mpsc::channel(16);
            let (handshake_tx, mut handshake_rx) = mpsc::channel(1);

            tokio::spawn(async move {
                if let Err(e) = socket_loop(socket, handshake_tx, receiver).await {
                    error!("error: {:?}", e);
                }
            });

            info!("Waiting for the handshake...");
            handshake_rx.recv().await;

            Ok(Box::new(Connection {
                sender_channel: sender
            }))
        },
        Err(e) => {
            error!("Error {:?}", e);
            Err(Box::new(e))
        }
    }
}

async fn socket_loop(socket: TcpStream, handshake_tx: mpsc::Sender<bool>, mut receiver: mpsc::Receiver<AMQPFrame>) -> Result<()> {
    let codec = AMQPCodec{};
    let (mut sink, mut stream) = Framed::new(socket, codec).split();

    if let Err(e) = sink.send(AMQPFrame::AMQPHeader).await {
        error!("What {}", e);

        return Err(Box::new(e))
    }

    loop {
        tokio::select! {
            result = stream.next() => {
                match result {
                    Some(Ok(frame)) => {
                        if let AMQPFrame::Method(_, class, method, _) = frame {
                            info!("Incoming frame {:04X}, {:04X}", class, method);
                        }

                        if let Some(response) = process_frame(frame) {
                            match response {
                                AMQPFrame::Method(_, 0x000A, 0x001F, _) =>
                                    handshake_tx.send(true).await?,
                                _ =>
                                    ()
                            }

                            sink.send(response).await?
                        }
                    },
                    x => {
                        panic!("Handle errors {:?}", x)
                    }
                }
            }
            Some(frame) = receiver.recv() => {
                info!("Loop got msg {:?}", frame);

                sink.send(frame).await?
            }
        }
    }
}

// sink: &mut futures::stream::SplitSink<tokio_util::codec::Framed<tokio::net::TcpStream, AMQPCodec>, AMQPFrame>
fn process_frame(frame: AMQPFrame) -> Option<AMQPFrame> {
    match frame {
        AMQPFrame::Method(channel, class, method, args) => {
            let cm = ((class as u32) << 16) | method as u32;
            response_to_method_frame(channel, cm, args.to_vec())
        },
        _ =>
            panic!("Uncovered branch")
    }
}

fn response_to_method_frame(channel: u16, cm: u32, args: Vec<AMQPValue>) -> Option<AMQPFrame> {
    match cm {
        frame::CONNECTION_START =>
            Some(connection_start_ok(channel)),
        frame::CONNECTION_TUNE =>
            Some(connection_tune_ok(channel)),
        _ =>
            None
    }
}

pub async fn open(connection: &Connection, virtual_host: String) -> Result<()> {
    connection.sender_channel.send(connection_open(0u16, virtual_host)).await?;
    Ok(())
}

fn connection_start_ok(channel: u16) -> AMQPFrame {
    let mut capabilities = Vec::<(String, AMQPFieldValue)>::new();

    capabilities.push(("authentication_failure_on_close".into(), AMQPFieldValue::Bool(true)));
    capabilities.push(("basic.nack".into(), AMQPFieldValue::Bool(true)));
    capabilities.push(("connection.blocked".into(), AMQPFieldValue::Bool(true)));
    capabilities.push(("consumer_cancel_notify".into(), AMQPFieldValue::Bool(true)));
    capabilities.push(("publisher_confirms".into(), AMQPFieldValue::Bool(true)));

    let mut client_properties = Vec::<(String, AMQPFieldValue)>::new();

    client_properties.push(("product".into(), AMQPFieldValue::LongString("ironmq-client".into())));
    client_properties.push(("platform".into(), AMQPFieldValue::LongString("Rust".into())));
    client_properties.push(("capabilities".into(), AMQPFieldValue::FieldTable(Box::new(capabilities))));
    client_properties.push(("version".into(), AMQPFieldValue::LongString("0.1.0".into())));

    let mut auth = Vec::new();
    auth.extend_from_slice(b"\x00guest\x00guest");

    let auth_string = String::from_utf8(auth).unwrap();

    let args = vec![
        AMQPValue::FieldTable(Box::new(client_properties)),
        AMQPValue::SimpleString("PLAIN".into()),
        AMQPValue::LongString(auth_string),
        AMQPValue::SimpleString("en_US".into()),
    ];

    AMQPFrame::Method(channel, 0x000A, 0x000B, Box::new(args))
}

fn connection_tune_ok(channel: u16) -> AMQPFrame {
    let args = vec![
        AMQPValue::U16(2047),
        AMQPValue::U32(131_072),
        AMQPValue::U16(60),
    ];

    AMQPFrame::Method(channel, 0x000A, 0x001F, Box::new(args))
}

fn connection_open(channel: u16, virtual_host: String) -> AMQPFrame {
    let args = vec![
        AMQPValue::SimpleString(virtual_host),
        AMQPValue::SimpleString("".into()),
        AMQPValue::U8(1u8)
    ];

    AMQPFrame::Method(channel, 0x000A, 0x0028, Box::new(args))
}
