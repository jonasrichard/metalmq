use crate::codec::{AMQPCodec, AMQPFieldValue, AMQPFrame, AMQPValue};
use crate::frame;
use futures::SinkExt;
use futures::stream::{StreamExt};
use log::{info, error};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::prelude::*;
use tokio::sync::mpsc;
use tokio_util::codec::{Framed};

// TODO have an own Error type
//
struct ConnectionState {
    sender_channel: Arc<mpsc::Sender<AMQPFrame>>,
    receiver_channel: Arc<mpsc::Receiver<AMQPFrame>>,
}

struct ChannelState {
}

pub trait Connection {
    fn open(&self, virtual_host: String);
}

pub trait Channel {
    fn basic_publish(&self, data: [u8]);
}

pub async fn connect(url: String) -> Result<Box<dyn Connection>, Box<dyn std::error::Error>> {
    match TcpStream::connect("127.0.0.1:5672").await {
        Ok(mut socket) => {
            let (s, r) = mpsc::channel(16);

            let sender = Arc::new(s);
            let receiver = Arc::new(r);

            let cloned_sender = sender.clone();
            let cloned_receiver = receiver.clone();

            tokio::spawn(async move {
                socket_loop(socket, cloned_sender, cloned_receiver)
            });

            Ok(Box::new(ConnectionState {
                sender_channel: sender,
                receiver_channel: receiver
            }))
        },
        Err(e) => {
            error!("Error {:?}", e);
            Err(Box::new(e))
        }
    }
}

async fn socket_loop(socket: TcpStream, sender: Arc<mpsc::Sender<AMQPFrame>>, receiver: Arc<mpsc::Receiver<AMQPFrame>>) -> Result<(), Box<dyn std::error::Error>> {
    let codec = AMQPCodec{};
    let (mut sink, mut stream) = Framed::new(socket, codec).split();

    sink.send(AMQPFrame::AMQPHeader).await?;

    while let Some(Ok(event)) = stream.next().await {
        info!("Event is {:?}", event);

        match event {
            AMQPFrame::Method(channel, class, method, args) =>
                if let Some(response) = response_to_method_frame(channel, ((class as u32) << 16usize) | method as u32, args.to_vec()) {
                    sink.send(response).await?;
                    ()
                },
            _ =>
                unimplemented!()
        }
    }

    Ok(())
}

fn response_to_method_frame(channel: u16, cm: u32, args: Vec<AMQPValue>) -> Option<AMQPFrame> {
    match cm {
        frame::ConnectionStart =>
            Some(connection_start_ok(channel)),
        frame::ConnectionTune =>
            Some(connection_tune_ok(channel)),
        _ =>
            None
    }
}

impl Connection for ConnectionState {
    fn open(&self, virtual_host: String) {

    }
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
