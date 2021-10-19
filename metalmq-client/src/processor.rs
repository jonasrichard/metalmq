use crate::client_api::{self, ClientRequest, Param};
use crate::client_error;
use crate::state;
use anyhow::Result;
use futures::stream::{SplitSink, StreamExt};
use futures::SinkExt;
use log::{debug, error, info, trace};
use metalmq_codec::codec::{AMQPCodec, Frame};
use metalmq_codec::frame;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::Framed;

pub(crate) async fn socket_loop(
    socket: TcpStream,
    mut requests: mpsc::Receiver<client_api::ClientRequest>,
) -> Result<()> {
    let (mut sink, mut stream) = Framed::new(socket, AMQPCodec {}).split();
    let (out_tx, mut out_rx) = mpsc::channel(1);
    let mut client = state::new(out_tx.clone());
    let feedback = Arc::new(Mutex::new(HashMap::<u16, client_api::Response>::new()));

    // I/O output port, handles outgoing frames sent via a channel.
    tokio::spawn(async move {
        if let Err(e) = handle_outgoing(&mut sink, &mut out_rx).await {
            error!("Error {:?}", e);
        }
    });

    // I/O input loop
    loop {
        tokio::select! {
            // Receiving incoming frames. Here we can handle any IO error and the
            // closing of the input stream (server closes the stream).
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
                        info!("Incoming client request {:?}", request);

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
    request: client_api::ClientRequest,
    mut client: &mut state::ClientState,
    feedback: &Arc<Mutex<HashMap<u16, client_api::Response>>>,
    outgoing: &mpsc::Sender<Frame>,
) -> Result<()> {
    use frame::{AMQPFrame, MethodFrameArgs};

    match request.param {
        Param::Frame(AMQPFrame::Header) => {
            register_waiter(feedback, Some(0), request.response);
            outgoing.send(Frame::Frame(AMQPFrame::Header)).await?;
        }
        Param::Frame(AMQPFrame::Method(ch, _, ma)) => {
            handle_out_frame(ch, ma, &mut client).await?;
            register_waiter(feedback, Some(ch), request.response);
        }
        Param::Consume(AMQPFrame::Method(ch, _, MethodFrameArgs::BasicConsume(args)), msg_sink) => {
            client.basic_consume(ch, args, msg_sink).await?;

            register_waiter(feedback, Some(ch), request.response);
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
fn notify_waiter(frame: &frame::AMQPFrame, feedback: &Arc<Mutex<HashMap<u16, client_api::Response>>>) -> Result<()> {
    use frame::AMQPFrame;

    trace!("Notify waiter by {:?}", frame);

    let r = match frame {
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
    };

    trace!("End of notify block");

    r
}

fn register_waiter(
    feedback: &Arc<Mutex<HashMap<u16, client_api::Response>>>,
    channel: Option<frame::Channel>,
    response_channel: Option<client_api::Response>,
) {
    trace!("Register waiter on channel {:?}", channel);

    if let Some(ch) = channel {
        if let Some(chan) = response_channel {
            feedback.lock().unwrap().insert(ch, chan);
        }
    }
}

async fn handle_in_frame(f: frame::AMQPFrame, cs: &mut state::ClientState) -> Result<()> {
    use frame::AMQPFrame::*;

    debug!("Incoming frame {:?}", f);

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
        ChannelOpenOk => cs.channel_open_ok(channel).await,
        ChannelCloseOk => cs.channel_close_ok(channel).await,
        ExchangeDeclareOk => cs.exchange_declare_ok().await,
        ExchangeDeleteOk => cs.exchange_delete_ok().await,
        QueueDeclareOk(args) => cs.queue_declare_ok(args).await,
        QueueBindOk => cs.queue_bind_ok().await,
        QueueUnbindOk => cs.queue_unbind_ok().await,
        QueueDeleteOk(args) => cs.queue_delete_ok(channel, args).await,
        ConnectionCloseOk => cs.connection_close_ok().await,
        BasicAck(args) => cs.basic_ack(channel, args).await,
        BasicConsumeOk(args) => cs.basic_consume_ok(args).await,
        BasicCancelOk(args) => cs.basic_cancel_ok(channel, args).await,
        BasicDeliver(args) => cs.basic_deliver(channel, args).await,
        ChannelClose(args) => cs.handle_channel_close(channel, args).await,
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
        ConnectionStartOk(args) => cs.connection_start_ok(args).await,
        ConnectionTuneOk(args) => cs.connection_tune_ok(args).await,
        ConnectionOpen(args) => cs.connection_open(args).await,
        ConnectionClose(args) => cs.connection_close(args).await,
        ChannelOpen => cs.channel_open(channel).await,
        ChannelClose(args) => cs.channel_close(channel, args).await,
        ExchangeDeclare(args) => cs.exchange_declare(channel, args).await,
        ExchangeDelete(args) => cs.exchange_delete(channel, args).await,
        QueueDeclare(args) => cs.queue_declare(channel, args).await,
        QueueBind(args) => cs.queue_bind(channel, args).await,
        QueueUnbind(args) => cs.queue_unbind(channel, args).await,
        QueueDelete(args) => cs.queue_delete(channel, args).await,
        BasicAck(args) => cs.basic_ack(channel, args).await,
        BasicCancel(args) => cs.basic_cancel(channel, args).await,
        _ => unimplemented!("{:?}", ma),
    }
}
pub(crate) async fn call(sink: &mpsc::Sender<ClientRequest>, f: frame::AMQPFrame) -> Result<()> {
    let (tx, rx) = oneshot::channel();

    sink.send(ClientRequest {
        param: Param::Frame(f),
        response: Some(tx),
    })
    .await?;

    rx.await?;

    Ok(())
}

pub(crate) async fn send(sink: &mpsc::Sender<ClientRequest>, f: frame::AMQPFrame) -> Result<()> {
    sink.send(ClientRequest {
        param: Param::Frame(f),
        response: None,
    })
    .await?;

    Ok(())
}
