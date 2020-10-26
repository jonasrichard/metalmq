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
    let mut buf = [0u8; 1024];

    info!("Connected to server");

    send_proto_header(&mut client).await?;

    loop {
        match client.socket.read(&mut buf).await {
            Ok(n) if n == 0 =>
                return Ok(()),
            Ok(n) => {
                let frame = parse_frame(&buf[0..n]);
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
fn parse_frame(buf: &[u8]) -> Option<Frame> {
    match buf.get(0) {
        Some(1) => {
            // check buffer length
            // why we need explicit types here?
            let channel = parse_u16(&buf, 1);
            let size = parse_u32(&buf, 3);
            let end = buf[7 + size as usize];

            info!("Payload size is {}", size);
            info!("End byte should be 0xCE {:?}", end);

            parse_method_frame(&buf[7..(size + 7) as usize], channel)
        },
        Some(_) =>
            None,
        None =>
            None
    }
}

fn parse_method_frame(buf: &[u8], channel: u16) -> Option<Frame> {
    let class_id = parse_u16(&buf, 0);
    let method_id = parse_u16(&buf, 2);
    let major_version = buf[4];
    let minor_version = buf[5];

    let field_table_size = parse_u32(&buf, 6);

    // we don't know the length of the string, so we don't know how
    // to go on with parsing
    // TODO use bytes or some buffer library
    let table_name = parse_simple_string(&buf, 10);

    match class_id {
        0x0A =>
            Some(Frame::Method(channel, MethodClass::Start, Method::Todo)),
        _ =>
            None
    }
}

fn parse_u16(buf: &[u8], i: usize) -> u16 {
    u16::from(buf[i]) << 8 | u16::from(buf[i+1])
}

fn parse_u32(buf: &[u8], i: usize) -> u32 {
    u32::from(buf[i]) << 24 |
        u32::from(buf[i+1]) << 16 |
        u32::from(buf[i+2]) << 8 |
        u32::from(buf[i+3])
}

fn parse_simple_string(buf: &[u8], i: usize) -> &str {
    let len = buf[i] as usize;

    std::str::from_utf8(&buf[1..len]).unwrap()
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
