use crate::client_sm::{self, ClientState};
use crate::{client_error, MessageSink};
use anyhow::{anyhow, Result};
use futures::stream::{SplitSink, StreamExt};
use futures::SinkExt;
use log::{debug, error, trace};
use metalmq_codec::codec::{AMQPCodec, Frame};
use metalmq_codec::frame::{self, AMQPFrame, MethodFrameArgs};
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::Framed;

pub(crate) type RequestSink = mpsc::Sender<Request>;

pub(crate) type MethodFrameCallback = dyn Fn(AMQPFrame) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync;

pub(crate) enum Param {
    Frame(AMQPFrame),
    FrameCallback(AMQPFrame, Box<MethodFrameCallback>),
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
            Param::FrameCallback(frame, _) => write!(f, "Request{{FrameCallback={:?}}}", frame),
            Param::Consume(frame, _) => write!(f, "Request{{Consume={:?}}}", frame),
            Param::Publish(frame, _) => write!(f, "Request{{Publish={:?}}}", frame),
        }
    }
}

pub(crate) async fn create_connection(url: String) -> Result<RequestSink> {
    match TcpStream::connect(url).await {
        Ok(socket) => {
            let (sender, receiver) = mpsc::channel(1);

            tokio::spawn(async move {
                if let Err(e) = socket_loop(socket, receiver).await {
                    error!("error: {:?}", e);
                }
            });

            Ok(sender)
        }
        Err(e) => Err(anyhow!("Connection error {:?}", e)),
    }
}

async fn socket_loop(socket: TcpStream, mut requests: mpsc::Receiver<Request>) -> Result<()> {
    let (mut sink, mut stream) = Framed::new(socket, AMQPCodec {}).split();
    let (out_tx, mut out_rx) = mpsc::channel(1);
    let mut client = client_sm::new(out_tx.clone());
    let feedback = Arc::new(Mutex::new(HashMap::<u16, Response>::new()));

    // TODO if server closes the stream, we need to notify all the async fns who are waiting
    // check what is the related error

    // TODO only feedback should be ArcMutex and client should register callbacks.
    // But if from those callback it wants to send messages, it will cause deadlock,
    // so probably the thread which executes the callbacks, should be a separated
    // thread.
    // TODO how to share client-state?
    //
    //   Incoming Frames              Client state             Outgoing Frames
    //         |                          |                          |
    //   ----->|     Decoded frame        |                          |
    //         |------------------------->|                          |
    //         |                          |Handle state changes      |
    //         |                          |                          |
    //         |                          |    Outgoing frames       |
    //         |                          |------------------------->|
    //         |                          |                          |
    //         |                          |                          |
    //         |                          |                          |
    //         |                          |                          |
    //         |                          |                          |
    //         |                          |                          |
    //         |                          |                          |
    //         |                          |                          |
    //         |                          |                          |
    //         |                          |                          |

    tokio::spawn(async move {
        if let Err(e) = handle_outgoing(&mut sink, &mut out_rx).await {
            error!("Error {:?}", e);
        }
    });

    loop {
        tokio::select! {
            incoming = stream.next() => {
                match incoming {
                    Some(Ok(Frame::Frame(frame))) => {
                        notify_waiter(&frame, &feedback)?;

                        if let Err(e) = handle_in_frame(frame, &mut client).await {
                            error!("Error {:?}", e);
                        }
                    }
                    Some(Ok(Frame::Frames(_))) => unimplemented!(),
                    Some(Err(e)) => {
                        error!("Error {:?}", e);
                    },
                    None => {
                        break;
                    }
                }
            }
            req = requests.recv() => {
                match req {
                    Some(request) => {
                        if let Err(e) = handle_request(request, &mut client, &feedback, &out_tx).await {
                            error!("Error {:?}", e);
                        }
                    },
                    None => {
                        // TODO client closed the stream, cleanup!
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

async fn handle_outgoing(
    sink: &mut SplitSink<Framed<TcpStream, AMQPCodec>, Frame>,
    outgoing: &mut mpsc::Receiver<Frame>,
) -> Result<()> {
    while let Some(f) = outgoing.recv().await {
        if let Err(e) = sink.send(f).await {
            error!("Error {:?}", e);
        }
    }

    Ok(())
}

// TODO we shouldn't register waiter always, only when the client wants blocking call
async fn handle_request(
    request: Request,
    mut client: &mut ClientState,
    feedback: &Arc<Mutex<HashMap<u16, Response>>>,
    outgoing: &mpsc::Sender<Frame>,
) -> Result<()> {
    match request.param {
        Param::Frame(AMQPFrame::Header) => {
            register_waiter(&feedback, Some(0), request.response);
            outgoing.send(Frame::Frame(AMQPFrame::Header)).await?;
        }
        Param::Frame(AMQPFrame::Method(ch, _, ma)) => {
            handle_out_frame(ch, ma, &mut client).await?;
            register_waiter(&feedback, Some(ch), request.response);
        }
        Param::Consume(AMQPFrame::Method(ch, _, MethodFrameArgs::BasicConsume(args)), msg_sink) => {
            client.basic_consume(ch, args, msg_sink).await?;

            register_waiter(&feedback, Some(ch), request.response);
        }
        Param::Publish(AMQPFrame::Method(ch, _, MethodFrameArgs::BasicPublish(args)), content) => {
            client.basic_publish(ch, args, content).await?;
        }
        _ => unreachable!("{:?}", request),
    }

    Ok(())
}

/// Unblock the client by sending a `Response`. If there is no error on the channel or
/// in the connection the result will be a unit type. If there is an AMQP channel error,
/// it sends back to the client call who is blocked on that channel, so the client API
/// will receive the `ClientError`. If there is a connection error, it notifies all
/// the calls who are waiting on channels (otherwise the client API would remain blocked)
/// and sends back the error to a random waiter. (Sorry, if I have a better idea, I fix this.)
fn notify_waiter(frame: &AMQPFrame, feedback: &Arc<Mutex<HashMap<u16, Response>>>) -> Result<()> {
    trace!("Notify waiter by {:?}", frame);

    let r = match frame {
        AMQPFrame::Method(_, frame::CONNECTION_CLOSE, MethodFrameArgs::ConnectionClose(args)) => {
            let err = crate::ClientError {
                channel: None,
                code: args.code,
                message: args.text.clone(),
                class_method: frame::unify_class_method(args.class_id, args.method_id),
            };

            for (_, fb) in feedback.lock().unwrap().drain() {
                if fb.send(Err(anyhow::Error::new(err.clone()))).is_err() {
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

            if let Some(fb) = feedback.lock().unwrap().remove(&channel) {
                if fb.send(err).is_err() {
                    return client_error!(None, 501, "Cannot unblock client", 0);
                }
            }
            Ok(())
        }
        AMQPFrame::Method(channel, _, _) => {
            if let Some(fb) = feedback.lock().unwrap().remove(&channel) {
                if fb.send(Ok(())).is_err() {
                    return client_error!(None, 501, "Cannot unblock client", 0);
                }
            }
            Ok(())
        }
        _ => Ok(()),
    };

    trace!("End of notify block");

    r
}

fn register_waiter(
    feedback: &Arc<Mutex<HashMap<u16, Response>>>,
    channel: Option<frame::Channel>,
    response_channel: Option<Response>,
) {
    trace!("Register waiter on channel {:?}", channel);

    if let Some(ch) = channel {
        if let Some(chan) = response_channel {
            feedback.lock().unwrap().insert(ch, chan);
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

async fn handle_in_frame(f: AMQPFrame, cs: &mut ClientState) -> Result<()> {
    debug!("Incoming frame {:?}", f);

    match f {
        AMQPFrame::Header => Ok(()),
        AMQPFrame::Method(ch, _, args) => handle_in_method_frame(ch, args, cs).await,
        AMQPFrame::ContentHeader(ch) => cs.content_header(ch).await,
        AMQPFrame::ContentBody(cb) => cs.content_body(cb).await,
        AMQPFrame::Heartbeat(_) => Ok(()),
    }
}

/// Handle AMQP frames coming from the server side
async fn handle_in_method_frame(
    channel: frame::Channel,
    ma: frame::MethodFrameArgs,
    cs: &mut ClientState,
) -> Result<()> {
    match ma {
        MethodFrameArgs::ConnectionStart(args) => cs.connection_start(args).await,
        MethodFrameArgs::ConnectionTune(args) => cs.connection_tune(args).await,
        MethodFrameArgs::ConnectionOpenOk => cs.connection_open_ok().await,
        MethodFrameArgs::ConnectionClose(args) => cs.handle_connection_close(args).await,
        MethodFrameArgs::ChannelOpenOk => cs.channel_open_ok(channel).await,
        MethodFrameArgs::ChannelCloseOk => cs.channel_close_ok(channel).await,
        MethodFrameArgs::ExchangeDeclareOk => cs.exchange_declare_ok().await,
        MethodFrameArgs::QueueDeclareOk(args) => cs.queue_declare_ok(args).await,
        MethodFrameArgs::QueueBindOk => cs.queue_bind_ok().await,
        MethodFrameArgs::ConnectionCloseOk => cs.connection_close_ok().await,
        MethodFrameArgs::BasicAck(args) => cs.basic_ack(channel, args).await,
        MethodFrameArgs::BasicConsumeOk(args) => cs.basic_consume_ok(args).await,
        MethodFrameArgs::BasicDeliver(args) => cs.basic_deliver(channel, args).await,
        MethodFrameArgs::ChannelClose(args) => cs.handle_channel_close(channel, args).await,
        //    // TODO check if client is consuming messages from that channel + consumer tag
        _ => unimplemented!("{:?}", ma),
    }
}

async fn handle_out_frame(channel: frame::Channel, ma: MethodFrameArgs, cs: &mut ClientState) -> Result<()> {
    debug!("Outgoing frame {:?}", ma);

    match ma {
        MethodFrameArgs::ConnectionStartOk(args) => cs.connection_start_ok(args).await,
        MethodFrameArgs::ConnectionTuneOk(args) => cs.connection_tune_ok(args).await,
        MethodFrameArgs::ConnectionOpen(args) => cs.connection_open(args).await,
        MethodFrameArgs::ConnectionClose(args) => cs.connection_close(args).await,
        MethodFrameArgs::ChannelOpen => cs.channel_open(channel).await,
        MethodFrameArgs::ChannelClose(args) => cs.channel_close(channel, args).await,
        MethodFrameArgs::ExchangeDeclare(args) => cs.exchange_declare(channel, args).await,
        MethodFrameArgs::QueueDeclare(args) => cs.queue_declare(channel, args).await,
        MethodFrameArgs::QueueBind(args) => cs.queue_bind(channel, args).await,
        MethodFrameArgs::BasicAck(args) => cs.basic_ack(channel, args).await,
        _ => unimplemented!(),
    }
}

pub(crate) async fn call(sink: &RequestSink, frame: AMQPFrame) -> Result<()> {
    let r = sink
        .send_timeout(
            Request {
                param: Param::Frame(frame),
                response: None,
            },
            tokio::time::Duration::from_secs(1),
        )
        .await;

    if let Err(e) = r {
        error!("Error {:?}", e);

        return Err(anyhow!(e));
    }

    Ok(())
}

pub(crate) async fn sync_call(sink: &RequestSink, frame: AMQPFrame) -> Result<()> {
    let (tx, rx) = oneshot::channel();

    let r = sink
        .send_timeout(
            Request {
                param: Param::Frame(frame),
                response: Some(tx),
            },
            tokio::time::Duration::from_secs(1),
        )
        .await;

    if let Err(e) = r {
        error!("Error {:?}", e);

        return Err(anyhow!(e));
    }

    match rx.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(_) => client_error!(None, 0, "Connection closed by peer", 0),
    }
}
