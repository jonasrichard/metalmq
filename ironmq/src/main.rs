use env_logger::Builder;
use futures::SinkExt;
use futures::stream::StreamExt;
use ironmq_codec::codec::{AMQPCodec, AMQPFrame};
use ironmq_codec::frame;
use log::{error, info};
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

    while let Some(payload) = stream.next().await {
        info!("Result {:?}", payload);

        match payload {
            Ok(frame) => {
                info!("Frame {:?}", frame);

                match frame {
                    AMQPFrame::AMQPHeader =>
                        sink.send(frame::connection_start(0u16)).await?,
                    AMQPFrame::Method(channel, class_method, args) => {
                        match class_method {
                            frame::CONNECTION_START_OK =>
                                sink.send(frame::connection_tune(channel)).await?,
                            frame::CONNECTION_TUNE_OK =>
                                (),
                            frame::CONNECTION_OPEN => {
                                info!("Open vhost {:?}", args[0]);
                                sink.send(frame::connection_open_ok(channel)).await?
                            },
                            frame::CONNECTION_CLOSE => {
                                sink.send(frame::connection_close_ok(channel)).await?;

                                return Ok(())
                            },
                            frame::CHANNEL_OPEN => {
                                info!("Open channel {}", channel);
                                sink.send(frame::channel_open_ok(channel)).await?
                            },
                            frame::EXCHANGE_DECLARE => {
                                info!("Exchange declare {:?} {:?}", args[1], args[2]);
                                sink.send(frame::exchange_declare_ok(channel)).await?
                            },
                            frame::BASIC_PUBLISH => {
                                info!("Publish {:?}", args[1])
                            },
                            m =>
                                panic!("Unsupported method frame {:?}", m)
                        }
                    },
                    AMQPFrame::ContentHeader(_channel, _class, _weight, size, _, _) =>
                        info!("Content header, size = {}", size),
                    AMQPFrame::ContentBody(_channel, bytes) =>
                        info!("Content {}", String::from_utf8(bytes.to_vec()).unwrap_or_default()),
                }
            },
            Err(e) =>
                return Err(Box::new(e))
        }
    }

    Ok(())
}

#[tokio::main]
pub async fn main() -> Result<()> {
    setup_logger();

    info!("Listening on port 5672");

    let listener = TcpListener::bind("127.0.0.1:5672").await?;

    loop {
        let (socket, _) = listener.accept().await?;

        tokio::spawn(async move {
            if let Err(e) = handle_client(socket).await {
                error!("Error handling client {:?}", e)
            }
        });
    }
}
