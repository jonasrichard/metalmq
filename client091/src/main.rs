mod codec;
mod frame;

use crate::codec::{AMQPFieldValue, AMQPFrame, AMQPValue};
use env_logger::Builder;
use futures::SinkExt;
use futures::stream::StreamExt;
use log::{info, error};
use std::io::Write;
use tokio::net::TcpStream;
use tokio::prelude::*;
use tokio_util::codec::{Framed};

struct ClientState {
    socket: TcpStream,
}

trait Client {
    fn connect(&self);
    fn basic_publish(&self, data: [u8]);
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

fn response_to_method_frame(channel: u16, cm: u32, args: Vec<AMQPValue>) -> Option<AMQPFrame> {
    match cm {
        frame::ConnectionStart =>
            Some(connection_start_ok(channel)),
        _ =>
            None
    }
}

async fn process_frames(mut client: ClientState) -> io::Result<()> {
    let codec = codec::AMQPCodec{};
    let (mut sink, mut stream) = Framed::new(&mut client.socket, codec).split();

    sink.send(codec::AMQPFrame::AMQPHeader).await?;

    while let Some(Ok(event)) = stream.next().await {
        info!("Event is {:?}", event);

        match event {
            AMQPFrame::AMQPHeader =>
                (),
            AMQPFrame::Method(channel, class, method, args) =>
                if let Some(response) = response_to_method_frame(channel, ((class as u32) << 16) | method as u32, args.to_vec()) {
                    sink.send(response).await?
                }
        }
    }

    Ok(())
}

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut builder = Builder::from_default_env();

    builder
        .format_timestamp_millis()
        .format(|buf, record| {
            writeln!(buf, "{} - [{}] :{} {}", buf.timestamp_millis(), record.level(),
                record.line().unwrap_or_default(), record.args())
        })
        .init();

    match TcpStream::connect("127.0.0.1:5672").await {
        Ok(socket) => {
            if let Err(e) = process_frames(ClientState {
                socket: socket,
            }).await {
               error!("Comm eror {:?}", e);
            }
            ()
        },
        Err(e) => {
            error!("Error {:?}", e)
        }
    }

    Ok(())
}
