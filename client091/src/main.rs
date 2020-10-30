use bytes::{BytesMut, Buf, BufMut};
use log::{info, error};
use tokio::net::TcpStream;
use tokio::prelude::*;

struct Client {
    socket: TcpStream,
}

type Channel = u16;

#[derive(Debug)]
enum Frame {
    Method(Channel, MethodClass, Method)
}

#[derive(Debug)]
enum MethodClass {
    Start
}

#[derive(Debug)]
enum Method {
    Todo
}

async fn process_frames(mut client: Client) -> io::Result<()> {
    let mut b = [0; 4096];
    let mut buf = BytesMut::with_capacity(65536);

    info!("Connected to server");

    send_proto_header(&mut client).await?;

    loop {
        match client.socket.read(&mut b).await {
            Ok(n) if n == 0 =>
                return Ok(()),
            Ok(n) => {
                buf.put(&b[0..n]);
                let frame = parse_frame(&mut buf);
                info!("Frame is {:?}", frame);
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
    let _major_version = buf.get_u8();
    let _minor_version = buf.get_u8();

    info!("Method id: {}", method_id);

    match class_id {
        0x0A => {
            let frame_len = buf.get_u32() as usize;
            info!("Frame content len {}", frame_len);

            let mut sub_buf = buf.split_to(frame_len);

            while sub_buf.has_remaining() {
                let table_name = parse_short_string(&mut sub_buf);
                info!("Table name {}", table_name);

                // TODO extract to parse value
                match sub_buf.get_u8() {
                    b'F' => {
                        parse_field_table(&mut sub_buf);
                        ()
                    },
                    b't' => {
                        let bool_value = sub_buf.get_u8();
                        info!("Bool: {}", bool_value);
                        ()
                    },
                    b'S' => {
                        let string_value = parse_long_string(&mut sub_buf);
                        info!("String: {}", string_value);
                        ()
                    },
                    _ =>
                        ()
                }
            }

            Some(Frame::Method(channel, MethodClass::Start, Method::Todo))
        },
        _ =>
            None
    }
}

fn parse_short_string(buf: &mut BytesMut) -> String {
    let len = buf.get_u8() as usize;
    let sb = buf.split_to(len);

    //info!("string len {}", len);

    //info!("string buffer {:?}", sb);

    String::from_utf8(sb.to_vec()).unwrap()
}

fn parse_long_string(buf: &mut BytesMut) -> String {
    let len = buf.get_u32() as usize;
    let sb = buf.split_to(len);

    String::from_utf8(sb.to_vec()).unwrap()
}

// TODO rename this
fn parse_field_table(buf: &mut BytesMut) {
    let len = buf.get_u32() as usize;
    let mut sub_buf = buf.split_to(len);

    while sub_buf.has_remaining() {
        let field_name = parse_short_string(&mut sub_buf);
        info!("Field name {}", field_name);

        match sub_buf.get_u8() {
            b't' => {
                let bool_value = sub_buf.get_u8();
                info!("Bool: {}", bool_value);
                ()
            },
            b'S' => {
                let string_value = parse_long_string(&mut sub_buf);
                info!("String: {}", string_value);
                ()
            },
            _ =>
                ()
        }

        info!("Remaining {}", sub_buf.remaining());
    }
}

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

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
