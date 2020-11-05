use bytes::{BytesMut, Buf, BufMut};
use env_logger::Builder;
use log::{info, error};
use std::collections::HashMap;
use std::io::Write;
use tokio::net::TcpStream;
use tokio::prelude::*;

struct Client {
    socket: TcpStream,
}

type Channel = u16;
type SimpleString = String;
type LongString = String;

#[derive(Debug)]
struct Argument {
    name: SimpleString,
    value: Value,
}

struct Frame {
    frame_type: u8, // ???
    channel: Channel,
    class: u16,  // ???
    method: u16, // ???
    arguments: Box<dyn Arguments>
}

trait Arguments {
}

/// Represents the Connection.Start method frame
#[derive(Debug)]
struct ConnectionStart {
    version_major: u8,
    version_minor: u8,
    server_properties: HashMap<SimpleString, Value>,
    mechanisms: String,
    locales: String
}

impl Arguments for ConnectionStart {
}

#[derive(Debug)]
enum Value {
    Bool(bool),
    Int(i32),
    FieldTable(HashMap<SimpleString, Value>),
    LongString(LongString)
}

async fn process_frames(mut client: Client) -> io::Result<()> {
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
                ()
            },
            Err(e) =>
                error!("Error reading frame {:?}", e)
        }
    }
}

async fn send_proto_header(client: &mut Client) -> io::Result<()> {
    client.socket.write_all(&[b'A', b'M', b'Q', b'P', 0, 0, 9, 1]).await?;

    Ok(())
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
    let class_id = buf.get_u16();
    let method_id = buf.get_u16();
    let major_version = buf.get_u8();
    let minor_version = buf.get_u8();

    info!("Method id: {}", method_id);

    match class_id {
        0x0A => {
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

            Some(Frame {
                frame_type: 1,
                channel: channel,
                class: class_id,
                method: 0x0A,
                arguments: Box::new(frame)
            })
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
            if let Err(e) = process_frames(Client {
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
