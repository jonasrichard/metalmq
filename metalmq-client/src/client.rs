use crate::client_sm::{self, ClientState};
use crate::{client_error, Client, MessageSink};
use anyhow::{anyhow, Result};
use futures::stream::StreamExt;
use futures::SinkExt;
use log::{debug, error};
use metalmq_codec::codec::AMQPCodec;
use metalmq_codec::frame::{self, AMQPFrame, MethodFrameArgs};
use std::collections::HashMap;
use std::fmt;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::Framed;

pub(crate) enum Param {
    Frame(AMQPFrame),
    Consume(AMQPFrame, MessageSink),
    Publish(AMQPFrame, Vec<u8>),
}

/// Response for passing errors to the client API.
pub(crate) type Response = oneshot::Sender<Result<()>>;

/// Represents a client request, typically send a frame and wait for the answer of the server.
pub(crate) struct Request {
    pub(crate) param: Param,
    pub(crate) response: Option<Response>,
}

impl fmt::Debug for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.param {
            Param::Frame(frame) => write!(f, "Request{{Frame={:?}}}", frame),
            Param::Consume(frame, _) => write!(f, "Request{{Consume={:?}}}", frame),
            Param::Publish(frame, _) => write!(f, "Request{{Publish={:?}}}", frame),
        }
    }
}

pub(crate) async fn create_connection(url: String) -> Result<Client> {
    match TcpStream::connect(url).await {
        Ok(socket) => {
            let (sender, receiver) = mpsc::channel(16);

            tokio::spawn(async move {
                if let Err(e) = socket_loop(socket, receiver).await {
                    error!("error: {:?}", e);
                }
            });

            Ok(Client { server_channel: sender })
        }
        Err(e) => Err(anyhow!("Connection error {:?}", e)),
    }
}

async fn socket_loop(socket: TcpStream, mut receiver: mpsc::Receiver<Request>) -> Result<()> {
    let (mut sink, mut stream) = Framed::new(socket, AMQPCodec {}).split();
    let mut client = client_sm::new();
    let mut feedback: HashMap<u16, Response> = HashMap::new();

    // TODO if server closes the stream, we need to notify all the async fns who are waiting
    // check what is the related error
    loop {
        tokio::select! {
            result = stream.next() => {
                match result {
                    Some(Ok(frame)) => {
                        notify_waiter(&frame, &mut feedback)?;

                        if let Ok(Some(response)) = handle_in_frame(&frame, &mut client).await {
                            sink.send(response).await?
                        }
                    },
                    Some(Err(e)) =>
                        error!("Handle errors {:?}", e),
                    None => {
                        debug!("Stream is closed normally");

                        return Ok(())
                    }
                }
            }
            Some(request) = receiver.recv() => {
                match request.param {
                    Param::Frame(AMQPFrame::Header) => {
                        register_waiter(&mut feedback, Some(0), request.response);
                        sink.send(AMQPFrame::Header).await?;
                    },
                    Param::Frame(AMQPFrame::Method(ch, _, ma)) =>
                        if let Some(response) = handle_out_frame(ch, ma, &mut client).await? {
                            let resp_channel = channel(&response);
                            sink.send(response).await?;
                            register_waiter(&mut feedback, resp_channel, request.response);
                        },
                    Param::Consume(AMQPFrame::Method(ch, _, MethodFrameArgs::BasicConsume(args)), msg_sink) =>
                        if let Some(response) = client.basic_consume(ch, &args, msg_sink).await? {
                            let resp_channel = channel(&response);
                            sink.send(response).await?;
                            register_waiter(&mut feedback, resp_channel, request.response);
                        },
                    Param::Publish(AMQPFrame::Method(ch, _, MethodFrameArgs::BasicPublish(args)), content) =>
                        for response in handle_publish(ch, args, content, &mut client).await? {
                            sink.send(response).await?;
                        },
                    _ =>
                        unreachable!("{:?}", request)
                }
            }
        }
    }
}

/// Unblock the client by sending a `Response`. If there is no error on the channel or
/// in the connection the result will be a unit type. If there is an AMQP channel error,
/// it sends back to the client call who is blocked on that channel, so the client API
/// will receive the `ClientError`. If there is a connection error, it notifies all
/// the calls who are waiting on channels (otherwise the client API would remain blocked)
/// and sends back the error to a random waiter. (Sorry, if I have a better idea, I fix this.)
fn notify_waiter(frame: &AMQPFrame, feedback: &mut HashMap<u16, Response>) -> Result<()> {
    match frame {
        AMQPFrame::Method(_, frame::CONNECTION_CLOSE, MethodFrameArgs::ConnectionClose(args)) => {
            let err = crate::ClientError {
                channel: None,
                code: args.code,
                message: args.text.clone(),
                class_method: frame::unify_class_method(args.class_id, args.method_id),
            };

            for (_, fb) in feedback.drain() {
                if let Err(_) = fb.send(Err(anyhow::Error::new(err.clone()))) {
                    // TODO what to do here?
                }
            }

            Ok(())
        }
        AMQPFrame::Method(channel, frame::CHANNEL_CLOSE, MethodFrameArgs::ChannelClose(args)) => {
            let err: Result<()> = client_error!(
                Some(*channel),
                args.code,
                args.text.clone(),
                frame::unify_class_method(args.class_id, args.method_id)
            );

            if let Some(fb) = feedback.remove(&channel) {
                if let Err(_) = fb.send(err) {
                    return client_error!(None, 501, "Cannot unblock client", 0);
                }
            }
            Ok(())
        }
        AMQPFrame::Method(channel, _, _) => {
            if let Some(fb) = feedback.remove(&channel) {
                if let Err(_) = fb.send(Ok(())) {
                    return client_error!(None, 501, "Cannot unblock client", 0);
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn register_waiter(
    feedback: &mut HashMap<u16, Response>,
    channel: Option<frame::Channel>,
    response_channel: Option<Response>,
) {
    if let Some(ch) = channel {
        if let Some(chan) = response_channel {
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

async fn handle_in_frame(f: &AMQPFrame, cs: &mut ClientState) -> Result<Option<AMQPFrame>> {
    debug!("Incoming frame {:?}", f);

    match f {
        AMQPFrame::Header => Ok(None),
        AMQPFrame::Method(ch, _, args) => {
            // TODO copy happens? check with a small poc
            handle_in_method_frame(*ch, args, cs).await
        }
        AMQPFrame::ContentHeader(ch) => cs.content_header(ch).await,
        AMQPFrame::ContentBody(cb) => cs.content_body(cb).await,
        AMQPFrame::Heartbeat(_) => Ok(None),
    }
}

/// Handle AMQP frames coming from the server side
async fn handle_in_method_frame(
    channel: frame::Channel,
    ma: &frame::MethodFrameArgs,
    cs: &mut ClientState,
) -> Result<Option<AMQPFrame>> {
    match ma {
        MethodFrameArgs::ConnectionStart(args) => cs.connection_start(args).await,
        MethodFrameArgs::ConnectionTune(args) => cs.connection_tune(args).await,
        MethodFrameArgs::ConnectionOpenOk => cs.connection_open_ok().await,
        MethodFrameArgs::ConnectionClose(args) => cs.handle_connection_close(args).await,
        MethodFrameArgs::ChannelOpenOk => cs.channel_open_ok(channel).await,
        MethodFrameArgs::ChannelCloseOk => cs.channel_close_ok(channel).await,
        MethodFrameArgs::ExchangeDeclareOk => cs.exchange_declare_ok().await,
        MethodFrameArgs::ExchangeBindOk => cs.exchange_bind_ok().await,
        MethodFrameArgs::QueueDeclareOk(args) => cs.queue_declare_ok(args).await,
        MethodFrameArgs::QueueBindOk => cs.queue_bind_ok().await,
        MethodFrameArgs::ConnectionCloseOk => cs.connection_close_ok().await,
        MethodFrameArgs::BasicConsumeOk(args) => cs.basic_consume_ok(args).await,
        MethodFrameArgs::BasicDeliver(args) => cs.basic_deliver(channel, args).await,
        MethodFrameArgs::ChannelClose(args) => cs.handle_channel_close(channel, args).await,
        //    // TODO check if client is consuming messages from that channel + consumer tag
        _ => unimplemented!("{:?}", ma),
    }
}

async fn handle_out_frame(
    channel: frame::Channel,
    ma: MethodFrameArgs,
    cs: &mut ClientState,
) -> Result<Option<AMQPFrame>> {
    debug!("Outgoing frame {:?}", ma);

    match ma {
        MethodFrameArgs::ConnectionStartOk(args) => cs.connection_start_ok(&args).await,
        MethodFrameArgs::ConnectionTuneOk(args) => cs.connection_tune_ok(&args).await,
        MethodFrameArgs::ConnectionOpen(args) => cs.connection_open(&args).await,
        MethodFrameArgs::ConnectionClose(args) => cs.connection_close(&args).await,
        MethodFrameArgs::ChannelOpen => cs.channel_open(channel).await,
        MethodFrameArgs::ChannelClose(args) => cs.channel_close(channel, &args).await,
        MethodFrameArgs::ExchangeDeclare(args) => cs.exchange_declare(channel, &args).await,
        MethodFrameArgs::QueueDeclare(args) => cs.queue_declare(channel, &args).await,
        MethodFrameArgs::QueueBind(args) => cs.queue_bind(channel, &args).await,
        MethodFrameArgs::BasicPublish(args) => cs.basic_publish(channel, &args).await,
        _ => unimplemented!(),
    }
}

async fn handle_publish(
    channel: frame::Channel,
    args: frame::BasicPublishArgs,
    content: Vec<u8>,
    cs: &mut ClientState,
) -> Result<Vec<AMQPFrame>> {
    match cs.basic_publish(channel, &args).await? {
        Some(publish_frame) => Ok(vec![
            publish_frame,
            AMQPFrame::ContentHeader(frame::content_header(channel, content.len() as u64)),
            AMQPFrame::ContentBody(frame::content_body(channel, content.as_slice())),
        ]),
        None => unreachable!(),
    }
}

pub(crate) async fn call(conn: &Client, frame: AMQPFrame) -> Result<()> {
    conn.server_channel
        .send(Request {
            param: Param::Frame(frame),
            response: None,
        })
        .await?;

    Ok(())
}

pub(crate) async fn sync_call(conn: &Client, frame: AMQPFrame) -> Result<()> {
    let (tx, rx) = oneshot::channel();

    conn.server_channel
        .send(Request {
            param: Param::Frame(frame),
            response: Some(tx),
        })
        .await?;

    match rx.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(_) => client_error!(None, 0, "Connection closed by peer", 0),
    }
}
