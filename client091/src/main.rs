use log::{info, error};
use tokio::net::TcpStream;
use tokio::prelude::*;

struct Client {
    socket: TcpStream,
}

#[derive(Debug)]
enum Frame {
    Method(u16, u16)
}

async fn process_frames(mut client: Client) -> io::Result<()> {
    let mut buf = [0u8; 1024];

    send_proto_header(&mut client).await;

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
            let channel: u16 = u16::from(buf[1]) << 8 | buf[2] as u16;
            // next 4 bytes are length
            let method: u16 = u16::from(buf[7]) << 8 | buf[8] as u16;

            Some(Frame::Method(channel, method))
        },
        Some(_) =>
            None,
        None =>
            None
    }
}

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let socket = TcpStream::connect("127.0.0.1:5672").await?;

    process_frames(Client {
        socket: socket,
    }).await?;

    Ok(())
}
