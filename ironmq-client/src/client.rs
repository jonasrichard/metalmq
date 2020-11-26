use crate::Result;
use crate::client_sm;
use futures::SinkExt;
use futures::stream::StreamExt;
use ironmq_codec::codec::{AMQPCodec, AMQPFrame, AMQPValue};
use ironmq_codec::frame;
use log::{info, error};
use std::fmt;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::Framed;

/// Represents a client request, typically send a frame and wait for the answer of the server.
struct Request {
    frame: AMQPFrame,
    response: Option<oneshot::Sender<AMQPFrame>>
}

impl fmt::Debug for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Request")
         .field("frame", &self.frame)
         .finish()
    }
}

pub struct Connection {
    server_channel: mpsc::Sender<Request>,
}

async fn create_connection(url: String) -> Result<Box<Connection>> {
    match TcpStream::connect(url).await {
        Ok(socket) => {
            let (sender, receiver) = mpsc::channel(16);

            tokio::spawn(async move {
                if let Err(e) = socket_loop(socket, receiver).await {
                    error!("error: {:?}", e);
                }
            });

            Ok(Box::new(Connection {
                server_channel: sender
            }))
        },
        Err(e) => {
            error!("Error {:?}", e);
            Err(Box::new(e))
        }
    }
}

async fn socket_loop(socket: TcpStream, mut receiver: mpsc::Receiver<Request>) -> Result<()> {
    let (mut sink, mut stream) = Framed::new(socket, AMQPCodec{}).split();
    let mut client_state = client_sm::ClientState{};

    loop {
        tokio::select! {
            result = stream.next() => {
                match result {
                    Some(Ok(frame)) => {
                        if let Ok(response) = handle_frame(frame, &mut client_state) {
                            sink.send(response).await?
                        }
                    },
                    Some(Err(e)) =>
                        error!("Handle errors {:?}", e),
                    None => {
                        info!("Connection is closed");

                        return Ok(())
                    }
                }
            }
            Some(request) = receiver.recv() => {
                // TODO here we need to implement that in a channel the messages are serialized
                // but we can have multiple channels
                sink.send(request.frame).await?;

                if let Some(response_channel) = request.response {
                    match stream.next().await {
                        Some(Ok(response_frame)) => {
                            response_channel.send(response_frame);
                            ()
                        },
                        Some(Err(e)) =>
                            error!("Handle error {:?}", e),
                        None =>
                            return Ok(())
                    }
                }
            }
        }
    }
}

fn handle_frame(input_frame: AMQPFrame, mut cs: &mut client_sm::ClientState) -> Result<AMQPFrame> {
    match &input_frame {
        AMQPFrame::Method(channel, cm, args) => {
            match *cm {
                frame::CONNECTION_START =>
                    client_sm::connection_start(&mut cs, input_frame.into()).map(|v| v.into()),
                frame::CONNECTION_TUNE =>
                    client_sm::connection_tune(&mut cs, input_frame.into()).map(|v| v.into()),
                _ =>
                    unimplemented!()
            }
        },
        _ =>
            unimplemented!()
    }
}

/// Connect to an AMQP server.
///
/// This is async code and wait for the Connection.Tune-Ok message.
///
/// ```no_run
/// let conn = client::connect("127.0.0.1:5672").await?;
/// ```
pub async fn connect(url: String) -> Result<Box<Connection>> {
    let connection = create_connection(url).await?;

    let (tx, rx) = oneshot::channel();
    let req = Request {
        frame: AMQPFrame::AMQPHeader,
        response: Some(tx)
    };

    connection.server_channel.send(req).await?;
    rx.await?;

    let (tx, rx) = oneshot::channel();
    let req = Request {
        frame: frame::connection_start_ok(0u16),
        response: Some(tx)
    };

    connection.server_channel.send(req).await?;
    // wait for the connection tune
    rx.await?;

    let req = Request {
        frame: frame::connection_tune_ok(0u16),
        response: None
    };
    connection.server_channel.send(req).await?;

    Ok(connection)
}

pub async fn open(connection: &Connection, virtual_host: String) -> Result<()> {
    let frame = frame::connection_open(0u16, virtual_host);
    let (tx, rx) = oneshot::channel();
    let req = Request {
        frame: frame,
        response: Some(tx)
    };

    connection.server_channel.send(req).await?;
    rx.await?;

    Ok(())
}

pub async fn close(connection: &Connection) -> Result<()> {
    let frame = frame::connection_close(0u16);
    let (tx, rx) = oneshot::channel();
    let req = Request {
        frame: frame,
        response: Some(tx)
    };

    connection.server_channel.send(req).await?;
    rx.await?;

    Ok(())
}

pub async fn channel_open(connection: &Connection, channel: u16) -> Result<()> {
    let frame = AMQPFrame::Method(channel, frame::CHANNEL_OPEN, Box::new(vec![AMQPValue::SimpleString("".into())]));
    let (tx, rx) = oneshot::channel();
    let req = Request {
        frame: frame,
        response: Some(tx)
    };

    connection.server_channel.send(req).await?;
    rx.await?;

    Ok(())
}

pub async fn exchange_declare(connection: &Connection, channel: u16, exchange_name: &str, exchange_type: &str) -> Result<()> {
    let (tx, rx) = oneshot::channel();
    let req = Request {
        frame: frame::exchange_declare(channel, exchange_name.into(), exchange_type.into()),
        response: Some(tx)
    };

    connection.server_channel.send(req).await?;
    rx.await?;

    Ok(())
}

pub async fn queue_bind(connection: &Connection, channel: u16, queue_name: &str, exchange_name: &str,
                        routing_key: &str) -> Result<()> {
    let (tx, rx) = oneshot::channel();
    connection.server_channel.send(Request {
        frame: frame::queue_bind(channel, queue_name.into(), exchange_name.into(), routing_key.into()),
        response: Some(tx)
    }).await?;
    rx.await?;

    Ok(())
}

pub async fn queue_declare(connection: &Connection, channel: u16, queue_name: &str) -> Result<()> {
    let (tx, rx) = oneshot::channel();
    connection.server_channel.send(Request {
        frame: frame::queue_declare(channel, queue_name.into()),
        response: Some(tx)
    }).await?;
    rx.await?;

    Ok(())
}

pub async fn basic_publish(connection: &Connection, channel: u16, exchange_name: String,
                           routing_key: String, payload: String) -> Result<()> {
    let bytes = payload.as_bytes();

    connection.server_channel.send(Request {
        frame: frame::basic_publish(channel, exchange_name, routing_key),
        response: None
    }).await?;

    connection.server_channel.send(Request {
        frame: frame::content_header(channel, bytes.len() as u64),
        response: None
    }).await?;

    connection.server_channel.send(Request {
        frame: frame::content_body(channel, bytes),
        response: None
    }).await?;

    Ok(())
}
