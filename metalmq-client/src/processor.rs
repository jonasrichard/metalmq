use crate::{
    client_api::{ConnectionSink, ConsumerSink},
    client_error, dev, state,
};
// TODO use thiserror here not anyhow
use anyhow::Result;
use futures::{
    stream::{SplitSink, Stream, StreamExt},
    SinkExt,
};
use log::{debug, error, trace};
use metalmq_codec::codec::{AMQPCodec, Frame};
use metalmq_codec::frame;
use std::collections::HashMap;
use std::io::Error;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::Framed;

#[cfg(test)]
mod processor_tests;

/// Represent a client request. It can be sending a frame, consume a queue or publish data.
pub(crate) enum Param {
    Connect {
        username: String,
        password: String,
        virtual_host: String,
        connected: oneshot::Sender<()>,
    },
    Frame(frame::AMQPFrame),
    Consume(frame::AMQPFrame, ConsumerSink),
    Publish(frame::AMQPFrame, Vec<u8>),
}

pub(crate) type FrameResponse = oneshot::Sender<Result<()>>;

pub(crate) enum WaitFor {
    // TODO this suggests an Option<WaitFor>
    Nothing,
    SentOut(oneshot::Sender<Result<()>>),
    FrameResponse(FrameResponse),
}

/// Represents a client request, typically send a frame and wait for the answer of the server.
pub(crate) struct ClientRequest {
    pub(crate) param: Param,
    pub(crate) response: WaitFor,
}

impl std::fmt::Debug for ClientRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.param {
            Param::Connect {
                username, virtual_host, ..
            } => write!(f, "Request{{Connect={:?},{:?}}}", username, virtual_host),
            Param::Frame(frame) => write!(f, "Request{{Frame={:?}}}", frame),
            Param::Consume(frame, _) => write!(f, "Request{{Consume={:?}}}", frame),
            Param::Publish(frame, _) => write!(f, "Request{{Publish={:?}}}", frame),
        }
    }
}

pub(crate) type ClientRequestSink = mpsc::Sender<ClientRequest>;

#[derive(Debug)]
pub(crate) struct OutgoingFrame {
    pub(crate) frame: Frame,
    pub(crate) written: Option<oneshot::Sender<Result<()>>>,
}

pub(crate) async fn start_loop_and_output(
    socket: TcpStream,
    requests: mpsc::Receiver<ClientRequest>,
    conn_evt_tx: ConnectionSink,
) -> Result<()> {
    let (mut sink, stream) = Framed::new(socket, AMQPCodec {}).split();
    let (out_tx, mut out_rx) = mpsc::channel(1);
    let client = state::new(out_tx, conn_evt_tx);

    // I/O output port, handles outgoing frames sent via a channel.
    tokio::spawn(async move {
        if let Err(e) = handle_outgoing(&mut sink, &mut out_rx).await {
            error!("Error {:?}", e);
        }
    });

    socket_loop(client, Box::pin(stream), requests).await
}

pub(crate) async fn socket_loop(
    mut client_state: state::ClientState,
    mut frame_stream: Pin<Box<dyn Stream<Item = Result<Frame, Error>> + Send>>,
    mut command_stream: mpsc::Receiver<ClientRequest>,
) -> Result<()> {
    let feedback = Arc::new(Mutex::new(HashMap::<u16, FrameResponse>::new()));

    loop {
        tokio::select! {
            // Receiving incoming frames. Here we can handle any IO error and the
            // closing of the input stream (server closes the stream).
            incoming = frame_stream.next() => {
                trace!("Incoming frame {:?}", incoming);

                match incoming {
                    Some(Ok(Frame::Frame(frame))) => {
                        debug!("Incoming frame {:?}", frame);

                        notify_waiter(&frame, &feedback).unwrap();

                        if let Err(e) = handle_in_frame(frame, &mut client_state).await {
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
            req = command_stream.recv() => {
                match req {
                    Some(request) => {
                        trace!("Client request {:?}", request);

                        if let Err(e) = handle_request(request, &mut client_state, &feedback).await {
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
    outgoing: &mut mpsc::Receiver<OutgoingFrame>,
) -> Result<()> {
    while let Some(f) = outgoing.recv().await {
        trace!("Sending out: {:?}", f);

        let OutgoingFrame { frame, written } = f;

        sink.send(frame).await.unwrap();
        if let Some(w) = written {
            w.send(Ok(())).unwrap();
        }
    }

    Ok(())
}

// TODO we shouldn't register waiter always, only when the client wants blocking call
async fn handle_request(
    request: ClientRequest,
    client: &mut state::ClientState,
    feedback: &Arc<Mutex<HashMap<u16, FrameResponse>>>,
) -> Result<()> {
    use frame::{AMQPFrame, MethodFrameArgs};

    match request.param {
        Param::Connect {
            username,
            password,
            virtual_host,
            connected,
        } => {
            client.start(username, password, virtual_host, connected).await?;
        }
        Param::Frame(AMQPFrame::Method(ch, _, MethodFrameArgs::BasicAck(args))) => {
            client.basic_ack(ch, args, request.response).await?;
        }
        Param::Frame(AMQPFrame::Method(ch, _, ma)) => {
            handle_out_frame(ch, ma, client).await?;
            register_wait_for(feedback, ch, request.response)?;
        }
        Param::Consume(AMQPFrame::Method(ch, _, MethodFrameArgs::BasicConsume(args)), msg_sink) => {
            client.basic_consume(ch, args, msg_sink).await?;
            register_wait_for(feedback, ch, request.response)?;
        }
        Param::Publish(AMQPFrame::Method(ch, _, MethodFrameArgs::BasicPublish(args)), content) => {
            client.basic_publish(ch, args, content).await?;
        }
        _ => unreachable!("{:?}", request),
    }

    Ok(())
}

fn register_wait_for(feedback: &Arc<Mutex<HashMap<u16, FrameResponse>>>, channel: u16, wf: WaitFor) -> Result<()> {
    match wf {
        WaitFor::Nothing => (),
        WaitFor::SentOut(tx) => {
            // Since the previous block has run, we sent out the frame.
            // TODO we need to send back send errors which we swallowed with ? operator
            // DOUBT can we send back here to the thread which basically sent a clientrequest
            // which is waiting for us to complete
            //if let Err(e) = tx.send(Ok(())) {
            //    error!("Error {:?}", e);
            //}
        }
        WaitFor::FrameResponse(tx) => {
            feedback.lock().unwrap().insert(channel, tx);
        }
    }

    Ok(())
}

/// Unblock the client by sending a `Response`. If there is no error on the channel or
/// in the connection the result will be a unit type. If there is an AMQP channel error,
/// it sends back to the client call who is blocked on that channel, so the client API
/// will receive the `ClientError`. If there is a connection error, it notifies all
/// the calls who are waiting on channels (otherwise the client API would remain blocked)
/// and sends back the error to a random waiter. (Sorry, if I have a better idea, I fix this.)
fn notify_waiter(frame: &frame::AMQPFrame, feedback: &Arc<Mutex<HashMap<u16, FrameResponse>>>) -> Result<()> {
    use frame::AMQPFrame;

    trace!("Notify waiter by {:?}", frame);

    match frame {
        AMQPFrame::Method(_, frame::CONNECTION_CLOSE, frame::MethodFrameArgs::ConnectionClose(args)) => {
            let err = crate::error::ClientError {
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
        AMQPFrame::Method(channel, frame::CHANNEL_CLOSE, frame::MethodFrameArgs::ChannelClose(args)) => {
            let err: Result<()> = client_error!(
                Some(*channel),
                args.code,
                args.text.clone(),
                frame::unify_class_method(args.class_id, args.method_id)
            );

            if let Some(fb) = feedback.lock().unwrap().remove(channel) {
                if fb.send(err).is_err() {
                    return client_error!(None, 501, "Cannot unblock client", 0);
                }
            }
            Ok(())
        }
        AMQPFrame::Method(channel, _, _) => {
            if let Some(fb) = feedback.lock().unwrap().remove(channel) {
                if fb.send(Ok(())).is_err() {
                    return client_error!(None, 501, "Cannot unblock client", 0);
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

async fn handle_in_frame(f: frame::AMQPFrame, cs: &mut state::ClientState) -> Result<()> {
    use frame::AMQPFrame::*;

    match f {
        Header => Ok(()),
        Method(ch, _, args) => handle_in_method_frame(ch, args, cs).await,
        ContentHeader(ch) => cs.content_header(ch).await,
        ContentBody(cb) => cs.content_body(cb).await,
        Heartbeat(_) => Ok(()),
    }
}

/// Handle AMQP frames coming from the server side
async fn handle_in_method_frame(
    channel: frame::Channel,
    ma: frame::MethodFrameArgs,
    cs: &mut state::ClientState,
) -> Result<()> {
    use frame::MethodFrameArgs::*;

    match ma {
        ConnectionStart(args) => cs.connection_start(args).await,
        ConnectionTune(args) => cs.connection_tune(args).await,
        ConnectionOpenOk => cs.connection_open_ok().await,
        ConnectionClose(args) => cs.handle_connection_close(args).await,
        ConnectionCloseOk => cs.connection_close_ok().await,
        ChannelOpenOk => cs.channel_open_ok(channel).await,
        ChannelClose(args) => cs.handle_channel_close(channel, args).await,
        ChannelCloseOk => cs.channel_close_ok(channel).await,
        ExchangeDeclareOk => cs.exchange_declare_ok().await,
        ExchangeDeleteOk => cs.exchange_delete_ok().await,
        QueueDeclareOk(args) => cs.queue_declare_ok(args).await,
        QueueBindOk => cs.queue_bind_ok().await,
        QueueUnbindOk => cs.queue_unbind_ok().await,
        QueuePurgeOk(_) => Ok(()),
        QueueDeleteOk(args) => cs.queue_delete_ok(channel, args).await,
        BasicAck(args) => cs.on_basic_ack(channel, args).await,
        BasicConsumeOk(args) => cs.basic_consume_ok(args).await,
        BasicCancelOk(args) => cs.basic_cancel_ok(channel, args).await,
        BasicDeliver(args) => cs.basic_deliver(channel, args).await,
        BasicReturn(args) => cs.on_basic_return(channel, args).await,
        ConfirmSelectOk => Ok(()),
        //    // TODO check if client is consuming messages from that channel + consumer tag
        _ => unimplemented!("{:?}", ma),
    }
}

async fn handle_out_frame(
    channel: frame::Channel,
    ma: frame::MethodFrameArgs,
    cs: &mut state::ClientState,
) -> Result<()> {
    use frame::MethodFrameArgs::*;

    debug!("Outgoing frame {:?}", ma);

    match ma {
        ConnectionClose(args) => cs.connection_close(args).await,
        ChannelOpen => cs.channel_open(channel).await,
        ChannelClose(args) => cs.channel_close(channel, args).await,
        ExchangeDeclare(args) => cs.exchange_declare(channel, args).await,
        ExchangeDelete(args) => cs.exchange_delete(channel, args).await,
        QueueDeclare(args) => cs.queue_declare(channel, args).await,
        QueueBind(args) => cs.queue_bind(channel, args).await,
        QueueUnbind(args) => cs.queue_unbind(channel, args).await,
        QueuePurge(args) => cs.queue_purge(channel, args).await,
        QueueDelete(args) => cs.queue_delete(channel, args).await,
        BasicCancel(args) => cs.basic_cancel(channel, args).await,
        ConfirmSelect(_) => cs.confirm_select(channel).await,
        _ => unimplemented!("{:?}", ma),
    }
}
pub(crate) async fn call(sink: &mpsc::Sender<ClientRequest>, f: frame::AMQPFrame) -> Result<()> {
    let (tx, rx) = oneshot::channel();

    log::trace!("Sending out a sync frame {:?}", f);

    dev::send_timeout(
        sink,
        ClientRequest {
            param: Param::Frame(f),
            response: WaitFor::FrameResponse(tx),
        },
    )
    .await
    .unwrap();

    rx.await.unwrap()?;

    Ok(())
}

pub(crate) async fn sync_send(sink: &mpsc::Sender<ClientRequest>, f: frame::AMQPFrame) -> Result<()> {
    let (tx, rx) = oneshot::channel();

    sink.send(ClientRequest {
        param: Param::Frame(f),
        response: WaitFor::SentOut(tx),
    })
    .await
    .unwrap();

    rx.await.unwrap()?;

    Ok(())
}

//pub(crate) async fn send(sink: &mpsc::Sender<ClientRequest>, f: frame::AMQPFrame) -> Result<()> {
//    sink.send(ClientRequest {
//        param: Param::Frame(f),
//        response: WaitFor::Nothing,
//    })
//    .await
//    .unwrap();
//
//    Ok(())
//}
