use crate::client_sm;
use crate::client_sm::{Client, ClientState};
use crate::{client_error, ConsumeCallback, Result};
use futures::stream::StreamExt;
use futures::SinkExt;
use ironmq_codec::codec::AMQPCodec;
use ironmq_codec::frame;
use ironmq_codec::frame::{AMQPFrame, MethodFrameArgs};
use log::{debug, error};
use std::collections::HashMap;
use std::fmt;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::Framed;

enum Param {
    Frame(AMQPFrame),
    Consume(AMQPFrame, ConsumeCallback),
    Publish(AMQPFrame, Vec<u8>)
}

/// Represents a client request, typically send a frame and wait for the answer of the server.
struct Request {
    param: Param,
    response: Option<oneshot::Sender<()>>,
}

impl fmt::Debug for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Request")
         .field("param", match &self.param {
             Param::Frame(frame) => frame,
             Param::Consume(frame, _) => frame,
             Param::Publish(frame, _) => frame
         })
         .finish()
    }
}

/// Represents a connection to AMQP server. It is not a trait since async functions in a trait
/// are not yet supported.
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
                        notify_waiter(&frame, &mut feedback)?;

                        if let Ok(Some(response)) = handle_in_frame(frame, &mut client) {
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
                match request.param {
                    Param::Frame(AMQPFrame::Header) => {
                        register_waiter(&mut feedback, &AMQPFrame::Header, request.response);
                        sink.send(AMQPFrame::Header).await?;
                    },
                    Param::Frame(AMQPFrame::Method(ch, _, ma)) =>
                        if let Some(response) = handle_out_frame(ch, ma, &mut client)? {
                            register_waiter(&mut feedback, &response, request.response);
                            sink.send(response).await?;
                        },
                    Param::Consume(AMQPFrame::Method(ch, _, MethodFrameArgs::BasicConsume(args)), cb) =>
                        if let Some(response) = client.basic_consume(ch, args, cb)? {
                            register_waiter(&mut feedback, &response, request.response);
                            sink.send(response).await?;
                        },
                    Param::Publish(AMQPFrame::Method(ch, _, MethodFrameArgs::BasicPublish(args)), content) =>
                        for response in handle_publish(ch, args, content, &mut client)? {
                            sink.send(response).await?;
                        },
                    _ =>
                        unreachable!("{:?}", request)
                }
            }
        }
    }
}

fn notify_waiter(frame: &AMQPFrame, feedback: &mut HashMap<u16, oneshot::Sender<()>>) -> Result<()> {
    if let Some(ch) = channel(&frame) {
        if let Some(fb) = feedback.remove(&ch) {
            if let Err(_) = fb.send(()) {
                return client_error!(2, "Cannot unblock client")
            }
        }
    }

    Ok(())
}

fn register_waiter(
    feedback: &mut HashMap<u16, oneshot::Sender<()>>,
    frame: &AMQPFrame,
    response_channel: Option<oneshot::Sender<()>>
) {
    if let Some(chan) = response_channel {
        if let Some(ch) = channel(&frame) {
            feedback.insert(ch, chan);
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

fn handle_in_frame(f: AMQPFrame, cs: &mut ClientState) -> Result<Option<AMQPFrame>> {
    match f {
        AMQPFrame::Header => Ok(None),
        AMQPFrame::Method(ch, _, args) => {
            // TODO copy happens? check with a small poc
            handle_in_method_frame(ch, args, cs)
        }
        AMQPFrame::ContentHeader(ch) => cs.content_header(*ch),
        AMQPFrame::ContentBody(cb) => cs.content_body(*cb),
        AMQPFrame::Heartbeat(_) => Ok(None),
    }
}

/// Handle AMQP frames coming from the server side
fn handle_in_method_frame(
    channel: frame::Channel,
    ma: frame::MethodFrameArgs,
    cs: &mut ClientState,
) -> Result<Option<AMQPFrame>> {
    match ma {
        MethodFrameArgs::ConnectionStart(args) => cs.connection_start(args),
        MethodFrameArgs::ConnectionTune(args) => cs.connection_tune(args),
        MethodFrameArgs::ConnectionOpenOk => cs.connection_open_ok(),
        MethodFrameArgs::ChannelOpenOk => cs.channel_open_ok(channel),
        MethodFrameArgs::ExchangeDeclareOk => cs.exchange_declare_ok(),
        MethodFrameArgs::ExchangeBindOk => cs.exchange_bind_ok(),
        MethodFrameArgs::QueueDeclareOk(args) => cs.queue_declare_ok(args),
        MethodFrameArgs::QueueBindOk => cs.queue_bind_ok(),
        MethodFrameArgs::ConnectionCloseOk => cs.connection_close_ok(),
        MethodFrameArgs::BasicConsumeOk(args) => cs.basic_consume_ok(args),
        MethodFrameArgs::BasicDeliver(args) => cs.basic_deliver(args),
        //frame::CHANNEL_CLOSE => cs.channel_close(channel, args),
        //    // TODO check if client is consuming messages from that channel + consumer tag
        _ => unimplemented!("{:?}", ma),
    }
}

fn handle_out_frame(channel: frame::Channel, ma: MethodFrameArgs, cs: &mut ClientState) -> Result<Option<AMQPFrame>> {
    debug!("Outgoing frame is {:?}", ma);

    match ma {
        MethodFrameArgs::ConnectionStartOk(args) => cs.connection_start_ok(args),
        MethodFrameArgs::ConnectionTuneOk(args) => cs.connection_tune_ok(args),
        MethodFrameArgs::ConnectionOpen(args) => cs.connection_open(args),
        MethodFrameArgs::ConnectionClose(args) => cs.connection_close(args),
        MethodFrameArgs::ChannelOpen => cs.channel_open(channel),
        MethodFrameArgs::ExchangeDeclare(args) => cs.exchange_declare(channel, args),
        MethodFrameArgs::QueueDeclare(args) => cs.queue_declare(channel, args),
        MethodFrameArgs::QueueBind(args) => cs.queue_bind(channel, args),
        MethodFrameArgs::BasicPublish(args) => cs.basic_publish(channel, args),
        _ => unimplemented!()
        //Command::ExchangeDeclare(args) => client_sm::exchange_declare(&mut cs, *args),
        //Command::QueueDeclare(args) => client_sm::queue_declare(&mut cs, *args),
        //Command::QueueBind(args) => client_sm::queue_bind(&mut cs, *args),
        //Command::BasicConsume(args) => client_sm::basic_consume(&mut cs, *args),
    }
}

fn handle_publish(
    channel: frame::Channel,
    args: frame::BasicPublishArgs, content: Vec<u8>,
    cs: &mut ClientState
) -> Result<Vec<AMQPFrame>> {
    match cs.basic_publish(channel, args)? {
        Some(publish_frame) =>
            Ok(vec![
                publish_frame,
                AMQPFrame::ContentHeader(Box::new(frame::content_header(channel, content.len() as u64))),
                AMQPFrame::ContentBody(Box::new(frame::content_body(channel, content.as_slice())))
            ]),
        None =>
            unreachable!()
    }
}

async fn call(conn: &Connection, frame: AMQPFrame) -> Result<()> {
    conn.server_channel
        .send(Request {
            param: Param::Frame(frame),
            response: None,
        })
        .await?;

    Ok(())
}

async fn sync_call(conn: &Connection, frame: AMQPFrame) -> Result<()> {
    let (tx, rx) = oneshot::channel();

    conn.server_channel
        .send(Request {
            param: Param::Frame(frame),
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
    sync_call(&connection, frame::connection_open(0, virtual_host)).await?;

    Ok(())
}

pub async fn close(connection: &Connection) -> Result<()> {
    sync_call(&connection, frame::connection_close(0)).await?;

    Ok(())
}

pub async fn channel_open(connection: &Connection, channel: u16) -> Result<()> {
    sync_call(&connection, frame::channel_open(channel)).await?;

    Ok(())
}

pub async fn exchange_declare(
    connection: &Connection,
    channel: u16,
    exchange_name: &str,
    exchange_type: &str,
) -> Result<()> {
    let frame = frame::exchange_declare(channel, exchange_name.into(), exchange_type.into());

    sync_call(&connection, frame).await?;

    Ok(())
}

pub async fn queue_bind(
    connection: &Connection,
    channel: u16,
    queue_name: &str,
    exchange_name: &str,
    routing_key: &str,
) -> Result<()> {
    let frame = frame::queue_bind(channel, queue_name.into(), exchange_name.into(), routing_key.into());

    sync_call(&connection, frame).await?;

    Ok(())
}

pub async fn queue_declare(connection: &Connection, channel: u16, queue_name: &str) -> Result<()> {
    let frame = frame::queue_declare(channel, queue_name.into());

    sync_call(&connection, frame).await?;

    Ok(())
}

pub async fn basic_consume<'a>(
    connection: &Connection,
    channel: u16,
    queue_name: &'a str,
    consumer_tag: &'a str,
    cb: fn(String) -> String,
) -> Result<()> {
    let frame = frame::basic_consume(channel, queue_name.into(), consumer_tag.into());
    let (tx, rx) = oneshot::channel();

    connection.server_channel.send(Request {
        param: Param::Consume(frame, Box::new(cb)),
        response: Some(tx)
    }).await?;

    match rx.await {
        Ok(()) => Ok(()),
        Err(_) => client_error!(0, "Channel recv error"),
    }
}

pub async fn basic_publish(
    connection: &Connection,
    channel: u16,
    exchange_name: &str,
    routing_key: &str,
    payload: String,
) -> Result<()> {
    let frame = frame::basic_publish(channel, exchange_name.into(), routing_key.into());

    connection.server_channel.send(Request {
        param: Param::Publish(frame, payload.as_bytes().to_vec()),
        response: None
    }).await?;

    Ok(())
}
