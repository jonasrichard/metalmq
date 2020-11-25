use crate::Result;
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
    feedback: Option<oneshot::Sender<AMQPFrame>>
}

impl fmt::Debug for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Request")
         .field("frame", &self.frame)
         .finish()
    }
}

pub struct Connection {
    sender_channel: mpsc::Sender<Request>,
}

pub trait Channel {
    fn basic_publish(&self, data: [u8]);
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
                sender_channel: sender
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
    let mut requests = Vec::<(u32, Option<oneshot::Sender<AMQPFrame>>)>::new();
    let mut request_id = 0u32;

    //let client = client_sm::start().await;

    loop {
        tokio::select! {
            result = stream.next() => {
                match result {
                    Some(Ok(frame)) => {
                        if frame::from_server(&frame) {
                            match frame {
                                AMQPFrame::Method(_, _, _) => {
                                    if let Some((_id, feedback)) = requests.pop() {
                                        if let Some(channel) = feedback {
                                            // TODO check result
                                            if let Err(e) = channel.send(frame) {
                                                error!("Send error {:?}", e);

                                                return Ok(())
                                            }
                                        }
                                    }
                                },
                                _ =>
                                    ()
                            }
                        } else {
                            if let Some(response) = process_frame(frame) {
                                sink.send(response).await?
                            }
                        }
                    },
                    Some(Err(e)) => {
                        error!("Handle errors {:?}", e);

                        return Err(Box::new(e))
                    },
                    None => {
                        info!("Connection is closed");

                        return Ok(())
                    }
                }
            }
            Some(Request{frame, feedback}) = receiver.recv() => {
                sink.send(frame).await?;

                if let Some(_) = feedback {
                    requests.push((request_id, feedback));
                    request_id += 1;
                }
            }
        }
    }
}

// sink: &mut futures::stream::SplitSink<tokio_util::codec::Framed<tokio::net::TcpStream, AMQPCodec>, AMQPFrame>
fn process_frame(frame: AMQPFrame) -> Option<AMQPFrame> {
    match frame {
        AMQPFrame::Method(channel, class_method, args) => {
            response_to_method_frame(channel, class_method, args.to_vec())
        },
        _ =>
            panic!("Uncovered branch")
    }
}

fn response_to_method_frame(channel: u16, cm: u32, _args: Vec<AMQPValue>) -> Option<AMQPFrame> {
    match cm {
        frame::CONNECTION_START =>
            Some(frame::connection_start_ok(channel)),
        frame::CONNECTION_TUNE =>
            Some(frame::connection_tune_ok(channel)),
        _ =>
            None
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
        feedback: Some(tx)
    };

    connection.sender_channel.send(req).await?;
    rx.await?;

    let (tx, rx) = oneshot::channel();
    let req = Request {
        frame: frame::connection_start_ok(0u16),
        feedback: Some(tx)
    };

    connection.sender_channel.send(req).await?;
    // wait for the connection tune
    rx.await?;

    let req = Request {
        frame: frame::connection_tune_ok(0u16),
        feedback: None
    };
    connection.sender_channel.send(req).await?;

    Ok(connection)
}

pub async fn open(connection: &Connection, virtual_host: String) -> Result<()> {
    let frame = frame::connection_open(0u16, virtual_host);
    let (tx, rx) = oneshot::channel();
    let req = Request {
        frame: frame,
        feedback: Some(tx)
    };

    connection.sender_channel.send(req).await?;
    rx.await?;

    Ok(())
}

pub async fn close(connection: &Connection) -> Result<()> {
    let frame = frame::connection_close(0u16);
    let (tx, rx) = oneshot::channel();
    let req = Request {
        frame: frame,
        feedback: Some(tx)
    };

    connection.sender_channel.send(req).await?;
    rx.await?;

    Ok(())
}

pub async fn channel_open(connection: &Connection, channel: u16) -> Result<()> {
    let frame = AMQPFrame::Method(channel, frame::CHANNEL_OPEN, Box::new(vec![AMQPValue::SimpleString("".into())]));
    let (tx, rx) = oneshot::channel();
    let req = Request {
        frame: frame,
        feedback: Some(tx)
    };

    connection.sender_channel.send(req).await?;
    rx.await?;

    Ok(())
}

pub async fn exchange_declare(connection: &Connection, channel: u16, exchange_name: &str, exchange_type: &str) -> Result<()> {
    let (tx, rx) = oneshot::channel();
    let req = Request {
        frame: frame::exchange_declare(channel, exchange_name.into(), exchange_type.into()),
        feedback: Some(tx)
    };

    connection.sender_channel.send(req).await?;
    rx.await?;

    Ok(())
}

pub async fn queue_bind(connection: &Connection, channel: u16, queue_name: &str, exchange_name: &str,
                        routing_key: &str) -> Result<()> {
    let (tx, rx) = oneshot::channel();
    connection.sender_channel.send(Request {
        frame: frame::queue_bind(channel, queue_name.into(), exchange_name.into(), routing_key.into()),
        feedback: Some(tx)
    }).await?;
    rx.await?;

    Ok(())
}

pub async fn queue_declare(connection: &Connection, channel: u16, queue_name: &str) -> Result<()> {
    let (tx, rx) = oneshot::channel();
    connection.sender_channel.send(Request {
        frame: frame::queue_declare(channel, queue_name.into()),
        feedback: Some(tx)
    }).await?;
    rx.await?;

    Ok(())
}

pub async fn basic_publish(connection: &Connection, channel: u16, exchange_name: String,
                           routing_key: String, payload: String) -> Result<()> {
    let bytes = payload.as_bytes();

    connection.sender_channel.send(Request {
        frame: frame::basic_publish(channel, exchange_name, routing_key),
        feedback: None
    }).await?;

    connection.sender_channel.send(Request {
        frame: frame::content_header(channel, bytes.len() as u64),
        feedback: None
    }).await?;

    connection.sender_channel.send(Request {
        frame: frame::content_body(channel, bytes),
        feedback: None
    }).await?;

    Ok(())
}
