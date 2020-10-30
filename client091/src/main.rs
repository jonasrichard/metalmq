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

fn parse_method_frame(mut buf: &mut BytesMut, channel: u16) -> Option<Frame> {
    let class_id = buf.get_u16();
    let method_id = buf.get_u16();
    let _major_version = buf.get_u8();
    let _minor_version = buf.get_u8();
    let _field_table_size = buf.get_u32();
    let table_name = parse_simple_string(&mut buf);

    info!("Method id: {}", method_id);
    info!("Table name: {}", table_name);

    match class_id {
        0x0A =>
            Some(Frame::Method(channel, MethodClass::Start, Method::Todo)),
        _ =>
            None
    }
}

fn parse_simple_string(buf: &mut BytesMut) -> String {
    let len = buf.get_u8() as usize;
    let mut sb = vec![0; len];

    buf.copy_to_slice(sb.as_mut_slice());

    info!("{:?}", sb);

    String::from_utf8(sb).unwrap()
}

fn parse_field_table(mut buf: &mut BytesMut) {
    let len = buf.get_u32();
    let i = 4;

    while i < len {
        // TODO this conversion is not good
        let _field_name = parse_simple_string(&mut buf);
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
