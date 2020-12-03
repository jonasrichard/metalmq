use crate::{client_error, Result};
use crate::client_sm;
use crate::client_sm::{ClientState, Command};
use futures::SinkExt;
use futures::stream::StreamExt;
use ironmq_codec::codec::AMQPCodec;
use ironmq_codec::frame::{AMQPFrame, MethodFrame};
use ironmq_codec::frame;
use log::{debug, error};
use std::collections::HashMap;
use std::fmt;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::Framed;

#[derive(Debug)]
enum Param {
    Command(Command),
    Frame(AMQPFrame)
}

/// Represents a client request, typically send a frame and wait for the answer of the server.
struct Request {
    param: Param,
    response: Option<oneshot::Sender<()>>
}

impl fmt::Debug for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Request")
         .field("command", &self.param)
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
    let mut client_state = ClientState{
        state: client_sm::Phase::Uninitialized,
        username: "guest".into(),
        password: "guest".into()
    };
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

                        if let Ok(Some(response)) = handle_server_frame(frame, &mut client_state) {
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
                if let Ok(Some(frame_to_send)) = handle_client_request(request.param, &mut client_state) {
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
        AMQPFrame::AMQPHeader =>
            Some(0),
        AMQPFrame::Method(mf) =>
            Some(mf.channel),
        _ =>
            None
    }
}

fn handle_server_frame(f: AMQPFrame, mut cs: &mut ClientState) -> Result<Option<AMQPFrame>> {
    match f {
        AMQPFrame::AMQPHeader =>
            Ok(None),
        AMQPFrame::Method(mf) =>
            handle_server_method_frame(*mf, &mut cs),
        AMQPFrame::ContentHeader(_) =>
            Ok(None),
        AMQPFrame::ContentBody(_) =>
            Ok(None)
    }
}

fn handle_client_request(p: Param, mut cs: &mut ClientState) -> Result<Option<AMQPFrame>> {
    match p {
        Param::Command(c) =>
            handle_command(c, &mut cs),
        Param::Frame(_) =>
            // is it needed?
            Ok(None)
    }
}

/// Handle AMQP frames coming from the server side
fn handle_server_method_frame(mf: MethodFrame, mut cs: &mut ClientState) -> Result<Option<AMQPFrame>> {
    let result = match mf.class_method {
        frame::CONNECTION_START =>
            client_sm::connection_start(&mut cs, mf),
        frame::CONNECTION_TUNE =>
            client_sm::connection_tune(&mut cs, mf),
        frame::CONNECTION_OPEN_OK =>
            client_sm::connection_open_ok(&mut cs, mf),
        frame::CHANNEL_OPEN_OK =>
            client_sm::channel_open_ok(&mut cs, mf),
        frame::CHANNEL_CLOSE =>
            client_sm::channel_close(&mut cs, mf),
        frame::EXCHANGE_DECLARE_OK =>
            client_sm::exchange_declare_ok(&mut cs, mf),
        frame::QUEUE_DECLARE_OK =>
            client_sm::queue_declare_ok(&mut cs, mf),
        frame::QUEUE_BIND_OK =>
            client_sm::queue_bind_ok(&mut cs, mf),
        frame::CONNECTION_CLOSE_OK =>
            client_sm::connection_close_ok(&mut cs, mf),
        _ =>
            unimplemented!("{:?}", mf)
    };

    debug!("handle server frame {:?}", result);

    result.map(|of| of.map(|mf| AMQPFrame::Method(Box::new(mf))))
}

fn handle_command(cmd: Command, mut cs: &mut ClientState) -> Result<Option<AMQPFrame>> {
    debug!("Command is {:?}", cmd);

    if let Command::ConnectionInit = cmd {
        return Ok(Some(AMQPFrame::AMQPHeader))
    }

    let result = match cmd {
        Command::ConnectionInit =>
            unreachable!(),
        Command::ConnectionOpen(args) =>
            client_sm::connection_open(&mut cs, *args),
        Command::ConnectionClose =>
            client_sm::connection_close(&mut cs),
        Command::ConnectionStartOk =>
            client_sm::connection_start_ok(&mut cs),
        Command::ConnectionTuneOk =>
            client_sm::connection_tune_ok(&mut cs),
        Command::ChannelOpen(args) =>
            client_sm::channel_open(&mut cs, *args),
        Command::ExchangeDeclare(args) =>
            client_sm::exchange_declare(&mut cs, *args),
        Command::QueueDeclare(args) =>
            client_sm::queue_declare(&mut cs, *args),
        Command::QueueBind(args) =>
            client_sm::queue_bind(&mut cs, *args),
    };

    debug!("Result = {:?}", result);

    result.map(|o| o.map(|f| AMQPFrame::Method(Box::new(f))))
}

async fn call(conn: &Connection, cmd: Command) -> Result<()> {
    conn.server_channel.send(Request {
        param: Param::Command(cmd),
        response: None
    }).await?;

    Ok(())
}

async fn sync_call(conn: &Connection, cmd: Command) -> Result<()> {
    let (tx, rx) = oneshot::channel();

    conn.server_channel.send(Request {
        param: Param::Command(cmd),
        response: Some(tx)
    }).await?;

    match rx.await {
        Ok(()) =>
            Ok(()),
        Err(_) =>
            client_error!(0, "Channel recv error")
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

    sync_call(&connection, Command::ConnectionInit).await?;
    sync_call(&connection, Command::ConnectionStartOk).await?;
    call(&connection, Command::ConnectionTuneOk).await?;

    Ok(connection)
}

pub async fn open(connection: &Connection, virtual_host: String) -> Result<()> {
    let command = client_sm::ConnOpenArgs {
        virtual_host: virtual_host,
        insist: true
    };

    sync_call(&connection, Command::ConnectionOpen(Box::new(command))).await?;

    Ok(())
}

pub async fn close(connection: &Connection) -> Result<()> {
    sync_call(&connection, Command::ConnectionClose).await?;

    Ok(())
}

pub async fn channel_open(connection: &Connection, channel: u16) -> Result<()> {
    let command = client_sm::ChannelOpenArgs {
        channel: channel
    };
    sync_call(&connection, Command::ChannelOpen(Box::new(command))).await?;

    Ok(())
}

pub async fn exchange_declare(connection: &Connection, channel: u16, exchange_name: &str, exchange_type: &str) -> Result<()> {
    let command = client_sm::ExchangeDeclareArgs {
        channel: channel,
        exchange_name: exchange_name.into(),
        exchange_type: exchange_type.into()
    };

    sync_call(&connection, Command::ExchangeDeclare(Box::new(command))).await?;

    Ok(())
}

pub async fn queue_bind(connection: &Connection, channel: u16, queue_name: &str, exchange_name: &str,
                        routing_key: &str) -> Result<()> {
    let command = client_sm::QueueBindArgs {
        channel: channel,
        queue_name: queue_name.into(),
        exchange_name: exchange_name.into(),
        routing_key: routing_key.into()
    };

    sync_call(&connection, Command::QueueBind(Box::new(command))).await?;

    Ok(())
}

pub async fn queue_declare(connection: &Connection, channel: u16, queue_name: &str) -> Result<()> {
    let command = client_sm::QueueDeclareArgs {
        channel: channel,
        queue_name: queue_name.into()
    };

    sync_call(&connection, Command::QueueDeclare(Box::new(command))).await?;

    Ok(())
}

pub async fn basic_publish(connection: &Connection, channel: u16, exchange_name: String,
                           routing_key: String, payload: String) -> Result<()> {
    let bytes = payload.as_bytes();

    connection.server_channel.send(Request {
        param: Param::Frame(frame::basic_publish(channel, exchange_name, routing_key).into()),
        response: None
    }).await?;

    connection.server_channel.send(Request {
        param: Param::Frame(frame::content_header(channel, bytes.len() as u64).into()),
        response: None
    }).await?;

    connection.server_channel.send(Request {
        param: Param::Frame(frame::content_body(channel, bytes).into()),
        response: None
    }).await?;

    Ok(())
}
