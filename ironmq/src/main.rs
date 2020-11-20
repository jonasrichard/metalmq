use env_logger::Builder;
use futures::SinkExt;
use futures::stream::StreamExt;
use ironmq_codec::codec::{AMQPCodec, AMQPFrame};
use ironmq_codec::frame;
use log::info;
use std::io::Write;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub type Result<T> = std::result::Result<T, Error>;

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

async fn handle_client(socket: TcpStream) -> Result<()> {
    let (mut sink, mut stream) = Framed::new(socket, AMQPCodec{}).split();

    loop {
        tokio::select! {
            result = stream.next() => {
                match result {
                    Some(Ok(frame)) => {
                        info!("{:?}", frame);

                        match frame {
                            AMQPFrame::AMQPHeader =>
                                sink.send(frame::connection_start(0u16)).await?,
                            AMQPFrame::Method(channel, class_method, _args) => {
                                match class_method {
                                    frame::CONNECTION_START_OK =>
                                        sink.send(frame::connection_tune(channel)).await?,
                                    frame::CONNECTION_TUNE_OK =>
                                        (),
                                    m =>
                                        panic!("Unsupported method frame {:?}", m)
                                }
                            },
                            _ =>
                                panic!("Unsupported frame {:?}", frame)
                        }
                    },
                    None => {
                        info!("Closing connection");
                        return Ok(())
                    },
                    _ => {
                        panic!("Unknown result {:?}", result)
                    }
                }
            }
        }
    }
}

#[tokio::main]
pub async fn main() -> Result<()> {
    setup_logger();

    info!("Listening on port 5672");

    let listener = TcpListener::bind("127.0.0.1:5672").await?;

    loop {
        let (socket, _) = listener.accept().await?;

        tokio::spawn(async move {
            handle_client(socket).await
        });
    }
}
