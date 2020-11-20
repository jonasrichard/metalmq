use env_logger::Builder;
use log::{info};
use std::io::Write;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;

use ironmq_codec::frame;

fn setup_logger() {
    let mut builder = Builder::from_default_env();

    builder
        .format_timestamp_millis()
        .format(|buf, record| {
            writeln!(buf, "{} - [{}] {}:{} {}", buf.timestamp_millis(), record.level(),
                record.file().unwrap_or_default(), record.line().unwrap_or_default(), record.args())
        })
        .init();
}

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    setup_logger();

    info!("Listening on port 5672");

    let listener = TcpListener::bind("127.0.0.1:5672").await?;

    loop {
        let (mut socket, _) = listener.accept().await?;

        tokio::spawn(async move {
            let mut buf = [0; 1024];

            info!("Client connected");

            loop {
                let n = match socket.read(&mut buf).await {
                    Ok(n) if n == 0 => {
                        info!("Client disconnected");
                        return;
                    },
                    Ok(n) => {
                        dump(&buf[0..n]);
                        if validate(&buf[0..n]) {
                            if let Err(e) = write_header(&mut socket).await {
                                eprintln!("Error sending AMQP header {:?}", e);
                            }
                        }
                        n
                    },
                    Err(e) => {
                        eprintln!("failed to read from socket; err = {:?}", e);
                        return;
                    }
                };

                if let Err(e) = socket.write_all(&buf[0..n]).await {
                    eprintln!("failed to write socket; err = {:?}", e);
                    return;
                }
            }
        });
    }
}

fn dump(b: &[u8]) {
    let mut i = 0;
    let mut line = String::new();

    info!("[input] Length {}", b.len());
    info!("[input] {}", String::from_utf8(b.to_vec()).unwrap());

    while i < b.len() {
        line.push_str(&(format!("{:02X} ", b[i])));
        i += 1;
        if i % 16 == 0 {
            info!("[input] {}", line);
            line.clear();
        }
    }

    info!("[input] {}", line);
}

fn validate(frame: &[u8]) -> bool {
    let header = &[b'A', b'M', b'Q', b'P'];

    &frame[0..4] != header
}

async fn write_header(socket: &mut TcpStream) -> io::Result<()> {
    socket.write_all(&[b'A', b'M', b'Q', b'P', 0, 0, 9, 1]).await?;

    Ok(())
}
