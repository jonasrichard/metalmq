use crate::client_sm;
use crate::client_sm::{Client, ClientState};
use crate::{client_error, Result};
use futures::stream::StreamExt;
use futures::SinkExt;
use ironmq_codec::codec::AMQPCodec;
use ironmq_codec::frame;
use ironmq_codec::frame::{AMQPFieldValue, AMQPFrame, MethodFrameArgs};
use log::{debug, error};
use std::collections::HashMap;
use std::fmt;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::Framed;

/// Represents a client request, typically send a frame and wait for the answer of the server.
struct Request {
    frame: AMQPFrame,
    response: Option<oneshot::Sender<()>>,
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
                server_channel: sender,
            }))
        }
        Err(e) => {
            error!("Error {:?}", e);
            Err(Box::new(e))
        }
    }
}

async fn socket_loop(socket: TcpStream, mut receiver: mpsc::Receiver<Request>) -> Result<()> {
    let (mut sink, mut stream) = Framed::new(socket, AMQPCodec {}).split();
    let mut client = client_sm::new();
    let mut feedback: HashMap<u16, oneshot::Sender<()>> = HashMap::new();

    loop {
        tokio::select! {
            result = stream.next() => {
                match result {
                    Some(Ok(frame)) => {
                        let channel = channel(&frame);

                        // If someone waits for a message on this channel, notify them
                        if let Some(ch) = channel {
                            match feedback.remove(&ch) {
                                Some(fb) => {
                                    if let Err(_) = fb.send(()) {
                                        return client_error!(2, "Cannot unblock client")
                                    }
                                    ()
                                },
                                None =>
                                    ()
                            }
                        }

                        if let Ok(Some(response)) = handle_server_frame(frame, &mut client) {
                            sink.send(response).await?
                        }
                    },
                    Some(Err(e)) =>
                        error!("Handle errors {:?}", e),
                    None => {
                        return Ok(())
                    }
                }
            }
            Some(request) = receiver.recv() => {
                if let Ok(Some(frame_to_send)) = handle_client_request(request.frame, &mut client) {
                    debug!("Response frame {:?}", frame_to_send);

                    let channel = channel(&frame_to_send);

                    sink.send(frame_to_send).await?;

                    if let Some(response_channel) = request.response {
                        if let Some(ch) = channel {
                            feedback.insert(ch, response_channel);
                        }
                    }
                }
            }
        }
    }
}

fn channel(f: &AMQPFrame) -> Option<u16> {
    match f {
        AMQPFrame::Header => Some(0),
        AMQPFrame::Method(channel, _, _) => Some(*channel),
        _ => None,
    }
}

fn handle_server_frame(f: AMQPFrame, mut cs: &mut dyn Client) -> Result<Option<AMQPFrame>> {
    match f {
        AMQPFrame::Header => Ok(None),
        AMQPFrame::Method(ch, cm, args) => {
            // TODO copy happens? check with a small poc
            handle_server_method_frame(ch, cm, args, &mut cs)
        }
        AMQPFrame::ContentHeader(ch) => client_sm::content_header(&mut cs, *ch),
        AMQPFrame::ContentBody(cb) => client_sm::content_body(&mut cs, *cb),
        AMQPFrame::Heartbeat(_) => Ok(None),
    }
}

fn handle_client_request(f: AMQPFrame, mut cs: &mut dyn Client) -> Result<Option<AMQPFrame>> {
    match f {
        AMQPFrame::Method(ch, cm, args) => handle_command(ch, cm, args, &mut cs),
        _ => Ok(Some(f))
    }
}

/// Handle AMQP frames coming from the server side
fn handle_server_method_frame(
    channel: frame::Channel,
    class_method: frame::ClassMethod,
    args: frame::MethodFrameArgs,
    mut cs: &mut dyn Client,
) -> Result<Option<AMQPFrame>> {
    let result = match class_method {
        frame::CONNECTION_START => cs.connection_start(args),
        frame::CONNECTION_TUNE => cs.connection_tune(args),
        frame::CONNECTION_OPEN_OK => cs.connection_open_ok(args),
        frame::CONNECTION_CLOSE_OK => cs.connection_close_ok(args),
        frame::CHANNEL_OPEN_OK => cs.channel_open_ok(channel, args),
        frame::CHANNEL_CLOSE => cs.channel_close(channel, args),
        frame::EXCHANGE_DECLARE_OK => cs.exchange_declare_ok(channel, args),
        frame::QUEUE_DECLARE_OK => cs.queue_declare_ok(channel, args),
        frame::QUEUE_BIND_OK => cs.queue_bind_ok(channel, args),
        frame::BASIC_CONSUME_OK => cs.basic_consume_ok(channel, args),
        frame::BASIC_DELIVER => {
            // TODO check if client is consuming messages from that channel + consumer tag
            cs.basic_deliver(channel, args)
        }
        _ => unimplemented!("{}", class_method),
    };

    debug!("handle server frame {:?}", result);

    result.map(|of| of.map(|mf| AMQPFrame::Method(Box::new(mf))))
}

fn handle_command(args: MethodFrameArgs, mut cs: &mut dyn Client) -> Result<Option<AMQPFrame>> {
    debug!("Command is {:?}", args);

    let result = match cmd {
        Command::ConnectionInit => unreachable!(),
        Command::ConnectionOpen(args) => client_sm::connection_open(&mut cs, *args),
        Command::ConnectionClose => client_sm::connection_close(&mut cs),
        Command::ConnectionStartOk => client_sm::connection_start_ok(&mut cs),
        Command::ConnectionTuneOk => client_sm::connection_tune_ok(&mut cs),
        Command::ChannelOpen(args) => client_sm::channel_open(&mut cs, *args),
        Command::ExchangeDeclare(args) => client_sm::exchange_declare(&mut cs, *args),
        Command::QueueDeclare(args) => client_sm::queue_declare(&mut cs, *args),
        Command::QueueBind(args) => client_sm::queue_bind(&mut cs, *args),
        Command::BasicConsume(args) => client_sm::basic_consume(&mut cs, *args),
    };

    debug!("Result = {:?}", result);

    result.map(|o| o.map(|f| AMQPFrame::Method(Box::new(f))))
}

async fn call(conn: &Connection, frame: AMQPFrame) -> Result<()> {
    conn.server_channel
        .send(Request {
            frame: frame,
            response: None,
        })
        .await?;

    Ok(())
}

async fn sync_call(conn: &Connection, frame: AMQPFrame) -> Result<()> {
    let (tx, rx) = oneshot::channel();

    conn.server_channel
        .send(Request {
            frame: frame,
            response: Some(tx),
        })
        .await?;

    match rx.await {
        Ok(()) => Ok(()),
        Err(_) => client_error!(0, "Channel recv error"),
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

    sync_call(&connection, AMQPFrame::Header).await?;
    sync_call(
        &connection,
        frame::connection_start_ok("guest", "guest", HashMap::new()),
    )
    .await?;
    call(&connection, frame::connection_tune_ok(0)).await?;

    Ok(connection)
}

pub async fn open(connection: &Connection, virtual_host: String) -> Result<()> {
    let command = client_sm::ConnOpenArgs {
        virtual_host: virtual_host,
        insist: true,
    };

    sync_call(&connection, Command::ConnectionOpen(Box::new(command))).await?;

    Ok(())
}

pub async fn close(connection: &Connection) -> Result<()> {
    sync_call(&connection, Command::ConnectionClose).await?;

    Ok(())
}

pub async fn channel_open(connection: &Connection, channel: u16) -> Result<()> {
    let command = client_sm::ChannelOpenArgs { channel: channel };
    sync_call(&connection, Command::ChannelOpen(Box::new(command))).await?;

    Ok(())
}

pub async fn exchange_declare(
    connection: &Connection,
    channel: u16,
    exchange_name: &str,
    exchange_type: &str,
) -> Result<()> {
    let command = client_sm::ExchangeDeclareArgs {
        channel: channel,
        exchange_name: exchange_name.into(),
        exchange_type: exchange_type.into(),
    };

    sync_call(&connection, Command::ExchangeDeclare(Box::new(command))).await?;

    Ok(())
}

pub async fn queue_bind(
    connection: &Connection,
    channel: u16,
    queue_name: &str,
    exchange_name: &str,
    routing_key: &str,
) -> Result<()> {
    let command = client_sm::QueueBindArgs {
        channel: channel,
        queue_name: queue_name.into(),
        exchange_name: exchange_name.into(),
        routing_key: routing_key.into(),
    };

    sync_call(&connection, Command::QueueBind(Box::new(command))).await?;

    Ok(())
}

pub async fn queue_declare(connection: &Connection, channel: u16, queue_name: &str) -> Result<()> {
    let command = client_sm::QueueDeclareArgs {
        channel: channel,
        queue_name: queue_name.into(),
    };

    sync_call(&connection, Command::QueueDeclare(Box::new(command))).await?;

    Ok(())
}

pub async fn basic_consume<'a>(
    connection: &Connection,
    channel: u16,
    queue_name: &'a str,
    consumer_tag: &'a str,
    cb: fn(String) -> String,
) -> Result<()> {
    let command = client_sm::BasicConsumeArgs {
        channel: channel,
        queue_name: queue_name.into(),
        consumer_tag: consumer_tag.into(),
        no_local: false,
        no_ack: false,
        exclusive: false,
        no_wait: false,
        arguments: HashMap::<String, AMQPFieldValue>::new(),
        callback: Box::new(cb),
    };

    sync_call(&connection, Command::BasicConsume(Box::new(command))).await?;

    Ok(())
}

pub async fn basic_publish(
    connection: &Connection,
    channel: u16,
    exchange_name: &str,
    routing_key: &str,
    payload: String,
) -> Result<()> {
    connection
        .server_channel
        .send(Request {
            param: Param::SendContent(channel, exchange_name.into(), routing_key.into(), payload),
            response: None,
        })
        .await?;

        Param::SendContent(channel, exchange, routing_key, payload) => {
            let bytes = payload.as_bytes();

            let args = client_sm::BasicPublishArgs {
                channel: channel,
                exchange_name: exchange.clone(),
                routing_key: routing_key.clone(),
                mandatory: false,
            };

            client_sm::basic_publish(&mut cs, args);

            Ok(Some(vec![
                frame::basic_publish(channel, exchange, routing_key),
                AMQPFrame::ContentHeader(Box::new(frame::content_header(
                    channel,
                    bytes.len() as u64,
                ))),
                AMQPFrame::ContentBody(Box::new(frame::content_body(channel, bytes))),
            ]))
        }
    }
    Ok(())
}
