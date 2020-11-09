mod codec;

use bytes::{BytesMut, Buf, BufMut};
use env_logger::Builder;
use log::{info, error};
use std::collections::HashMap;
use std::io::Write;
use tokio::net::TcpStream;
use tokio::prelude::*;
use tokio::stream::StreamExt;
use tokio_util::codec::{Decoder, FramedRead};

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

fn handle_frame(mut client: ClientState, frame: Frame) {
    // match on the frame class and method
    match frame {
        Frame::MethodFrame(channel, class_method, args) =>
            match class_method {
                ClassMethod::ConnectionStart =>
                    (),
                    // send start-ok
                _ =>
                    ()
            },
        _ =>
            ()
    }
}

async fn process_frames(mut client: ClientState) -> io::Result<()> {
    let codec = codec::AMQPCodec{};
    //let (mut sink, mut stream) = codec.framed(client.socket).split();
    let mut stream = FramedRead::new(&mut client.socket, codec);

    // TODO use sink instead
    //send_proto_header(&mut client).await?;

    while let Some(Ok(event)) = stream.next().await {
        info!("Event is {:?}", event);
    }

    Ok(())
}
// TODO now we can implement the Frame enum -> [u8] conversion, so send should be easier

async fn process_frames2(mut client: ClientState) -> io::Result<()> {
    let mut b = [0; 4096];
    let mut buf = BytesMut::with_capacity(65536);

    info!("Connected to server");

    send_proto_header(&mut client).await?;

    loop {
        // We should create the fsm of the connection here which
        // processes the parsed frames
        match client.socket.read(&mut b).await {
            Ok(n) if n == 0 =>
                return Ok(()),
            Ok(n) => {
                buf.put(&b[0..n]);
                let _frame = parse_frame(&mut buf);

                // TODO refactor of course
                let mut auth = Vec::new();
                auth.extend_from_slice(b"\x00guest\x00guest");

                let mut capabilities = HashMap::new();
                capabilities.insert("authentication_failure_on_close".into(), Value::Bool(true));
                capabilities.insert("basic.nack".into(), Value::Bool(true));
                capabilities.insert("connection.blocked".into(), Value::Bool(true));
                capabilities.insert("consumer_cancel_notify".into(), Value::Bool(true));
                capabilities.insert("publisher_confirms".into(), Value::Bool(true));

                let mut client_properties: HashMap<String, Value> = HashMap::new();
                client_properties.insert("product".into(), Value::SimpleString("ironmq-client".into()));
                client_properties.insert("platform".into(), Value::SimpleString("Rust".into()));
                client_properties.insert("capabilities".into(), Value::FieldTable(capabilities));
                client_properties.insert("version".into(), Value::SimpleString("0.1.0".into()));

                let args = vec![
                    Argument { name: "Client-Properties".into(), value: Value::FieldTable(client_properties) },
                    Argument { name: "Mechanism".into(), value: Value::LongString(String::from_utf8(auth).unwrap()) },
                    Argument { name: "Response".into(), value: Value::SimpleString("PLAIN".into()) },
                    Argument { name: "Locale".into(), value: Value::SimpleString("en_US".into()) }
                ];

                let response = Frame::MethodFrame(0, ClassMethod::ConnectionStartOk, Box::new(args));
                send_frame(&mut client, response).await?;
                ()
            },
            Err(e) =>
                error!("Error reading frame {:?}", e)
        }
    }
}

async fn send_proto_header(client: &mut ClientState) -> io::Result<()> {
    client.socket.write_all(&[b'A', b'M', b'Q', b'P', 0, 0, 9, 1]).await?;

    Ok(())
}

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

fn write_value(mut buf: &mut BytesMut, value: &Value) {
    match value {
        Value::SimpleString(string) => {
            buf.put_u8(string.len() as u8);
            buf.put(string.as_bytes());
        },
        _ =>
            panic!("Unsupported type {:?}", value)
    }
}

// TODO should be result with an error message
fn parse_frame(mut buf: &mut BytesMut) -> Option<Frame> {
    match buf.get_u8() {
        1 => {
            // check buffer length
            // why we need explicit types here?
            let channel = buf.get_u16();
            let size = buf.get_u32();

            info!("Payload size is {}", size);

            parse_method_frame(&mut buf, channel)
        },
        _ =>
            None
    }
}

fn parse_method_frame(buf: &mut BytesMut, channel: u16) -> Option<Frame> {
    let class_method = buf.get_u32();
    let major_version = buf.get_u8();
    let minor_version = buf.get_u8();

    match class_method {
        0x000A000A => {
            let frame_len = buf.get_u32() as usize;
            let mut sub_buf = buf.split_to(frame_len);
            let mut props: HashMap<SimpleString, Value> = HashMap::new();

            while sub_buf.has_remaining() {
                let name = parse_short_string(&mut sub_buf);
                let value = parse_field_value(&mut sub_buf);

                props.insert(name, value);
            }

            let frame = ConnectionStart {
                version_major: major_version,
                version_minor: minor_version,
                server_properties: props,
                mechanisms: "".into(),
                locales: "".into()
            };

            // TODO convert struct to vec[args]

            Some(Frame::MethodFrame(channel, ClassMethod::ConnectionStart, Box::new(Vec::new())))
        },
        _ =>
            None
    }
}

fn parse_field_value(mut buf: &mut BytesMut) -> Value {
    match buf.get_u8() {
        b't' => {
            let bool_value = buf.get_u8() != 0;

            Value::Bool(bool_value)
        },
        b'S' => {
            let string_value = parse_long_string(&mut buf);

            Value::LongString(string_value)
        },
        b'F' => {
            let table = parse_field_table(&mut buf);

            Value::FieldTable(table)
        },
        t =>
            panic!("Unknown type {}", t)
    }
}

fn parse_short_string(buf: &mut BytesMut) -> String {
    let len = buf.get_u8() as usize;
    let sb = buf.split_to(len);

    String::from_utf8(sb.to_vec()).unwrap()
}

fn parse_long_string(buf: &mut BytesMut) -> String {
    let len = buf.get_u32() as usize;
    let sb = buf.split_to(len);

    String::from_utf8(sb.to_vec()).unwrap()
}

fn parse_field_table(buf: &mut BytesMut) -> HashMap<String, Value> {
    let len = buf.get_u32() as usize;
    let mut sub_buf = buf.split_to(len);
    let mut table = HashMap::new();

    while sub_buf.has_remaining() {
        let field_name = parse_short_string(&mut sub_buf);
        info!("Field name {}", field_name);

        let field_value = parse_field_value(&mut sub_buf);

        table.insert(field_name, field_value);
    }

    table
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
