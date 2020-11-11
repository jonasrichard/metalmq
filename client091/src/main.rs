mod codec;

use bytes::{BytesMut, BufMut};
use env_logger::Builder;
use futures::SinkExt;
use futures::stream::StreamExt;
use log::{info, error};
use std::collections::HashMap;
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

type FrameType = u8;
type Channel = u16;
type Class = u16;
type Method = u16;

type SimpleString = String;
type LongString = String;

#[derive(Debug)]
struct Argument {
    name: SimpleString,
    value: Value,
}

enum ClassMethod {
    ConnectionStart = 0x000A000A,
    ConnectionStartOk = 0x000A000B,
}

enum Frame {
    MethodFrame(Channel, ClassMethod, Box<Vec<Argument>>),
}

// It would be nice to cast Vec<Argument> and these structs easily
/// Represents the Connection.Start method frame
#[derive(Debug)]
struct ConnectionStart {
    version_major: u8,
    version_minor: u8,
    server_properties: HashMap<SimpleString, Value>,
    mechanisms: String,
    locales: String
}

struct ConnectionStartOk {
    client_properties: HashMap<SimpleString, Value>,
    mechanism: SimpleString,
    response: LongString,
    locale: SimpleString
}

#[derive(Debug)]
enum Value {
    Bool(bool),
    Int(i32),
    FieldTable(HashMap<String, Value>),
    SimpleString(SimpleString),
    LongString(LongString)
}

fn handle_frame(client: ClientState, frame: Frame) {
    // match on the frame class and method
    match frame {
        Frame::MethodFrame(_channel, class_method, _args) =>
            match class_method {
                ClassMethod::ConnectionStart =>
                    (),
                    // send start-ok
                _ =>
                    ()
            }
    }
}

async fn process_frames(mut client: ClientState) -> io::Result<()> {
    let codec = codec::AMQPCodec{};
    let (mut sink, mut stream) = Framed::new(&mut client.socket, codec).split();

    sink.send(codec::AMQPFrame::AMQPHeader).await?;

    while let Some(Ok(event)) = stream.next().await {
        info!("Event is {:?}", event);
    }

    Ok(())
}

//async fn process_frames2(mut client: ClientState) -> io::Result<()> {
//    let mut b = [0; 4096];
//    let mut buf = BytesMut::with_capacity(65536);
//
//    loop {
//        // We should create the fsm of the connection here which
//        // processes the parsed frames
//        match client.socket.read(&mut b).await {
//            Ok(n) if n == 0 =>
//                return Ok(()),
//            Ok(n) => {
//                buf.put(&b[0..n]);
//                let _frame = parse_frame(&mut buf);
//
//                // TODO refactor of course
//                let mut auth = Vec::new();
//                auth.extend_from_slice(b"\x00guest\x00guest");
//
//                let mut capabilities = HashMap::new();
//                capabilities.insert("authentication_failure_on_close".into(), Value::Bool(true));
//                capabilities.insert("basic.nack".into(), Value::Bool(true));
//                capabilities.insert("connection.blocked".into(), Value::Bool(true));
//                capabilities.insert("consumer_cancel_notify".into(), Value::Bool(true));
//                capabilities.insert("publisher_confirms".into(), Value::Bool(true));
//
//                let mut client_properties: HashMap<String, Value> = HashMap::new();
//                client_properties.insert("product".into(), Value::SimpleString("ironmq-client".into()));
//                client_properties.insert("platform".into(), Value::SimpleString("Rust".into()));
//                client_properties.insert("capabilities".into(), Value::FieldTable(capabilities));
//                client_properties.insert("version".into(), Value::SimpleString("0.1.0".into()));
//
//                let args = vec![
//                    Argument { name: "Client-Properties".into(), value: Value::FieldTable(client_properties) },
//                    Argument { name: "Mechanism".into(), value: Value::LongString(String::from_utf8(auth).unwrap()) },
//                    Argument { name: "Response".into(), value: Value::SimpleString("PLAIN".into()) },
//                    Argument { name: "Locale".into(), value: Value::SimpleString("en_US".into()) }
//                ];
//
//                let response = Frame::MethodFrame(0, ClassMethod::ConnectionStartOk, Box::new(args));
//                send_frame(&mut client, response).await?;
//                ()
//            },
//            Err(e) =>
//                error!("Error reading frame {:?}", e)
//        }
//    }
//}

async fn send_frame(client: &mut ClientState, frame: Frame) -> io::Result<()> {
    match frame {
        Frame::MethodFrame(channel, cm, args) => {
            let mut frame_buf = BytesMut::with_capacity(65563);
            frame_buf.put_u8(1);
            frame_buf.put_u16(0);

            let mut buf = BytesMut::with_capacity(65536);
            buf.put_u16(0x0A);
            buf.put_u16(0x0B);

            let mut arg_buf = BytesMut::with_capacity(65536);
            for arg in args.iter() {
                write_value(&mut arg_buf, &arg.value);
            }

            buf.put_u32(arg_buf.len() as u32);
            buf.put(arg_buf);

            frame_buf.put_u32(buf.len() as u32);
            frame_buf.put(buf);
            frame_buf.put_u8(0xCE);

            info!("{:?}", frame_buf)
        },
        _ =>
            panic!("Unknown frame")
    }

    Ok(())
}

fn write_value(buf: &mut BytesMut, value: &Value) {
    match value {
        Value::SimpleString(string) => {
            buf.put_u8(string.len() as u8);
            buf.put(string.as_bytes());
        },
        _ =>
            panic!("Unsupported type {:?}", value)
    }
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
