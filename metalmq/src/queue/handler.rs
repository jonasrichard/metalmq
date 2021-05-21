use crate::message::Message;
use crate::{ConsumerTag, Result};
use log::{debug, error, info};
use metalmq_codec::frame;
use std::collections::{HashMap, VecDeque};
use tokio::sync::{mpsc, oneshot};

pub(crate) type QueueCommandSink = mpsc::Sender<QueueCommand>;
pub(crate) type FrameSink = mpsc::Sender<frame::AMQPFrame>;
//pub(crate) type FrameStream = mpsc::Receiver<frame::AMQPFrame>;

#[derive(Debug)]
pub(crate) enum QueueCommand {
    Message(Message),
    Consume {
        consumer_tag: String,
        frame_sink: FrameSink,
        response: oneshot::Sender<()>,
    },
    Cancel {
        consumer_tag: String,
        response: oneshot::Sender<()>,
    },
}

pub(crate) async fn queue_loop(commands: &mut mpsc::Receiver<QueueCommand>) {
    // TODO we need to have a variable here to access the queue properties
    let mut messages = VecDeque::new();
    let mut consumers = HashMap::<ConsumerTag, FrameSink>::new();

    while let Some(command) = commands.recv().await {
        match command {
            QueueCommand::Message(message) => {
                debug!("Sending message to consumers {}", consumers.len());

                let mut has_sent = false;

                // Do we need to send to all consumers? No, it is fanout yes, but topic, direct no.
                for (consumer_tag, consumer) in &consumers {
                    let frames = message_to_frames(&message, consumer_tag);

                    if let Ok(true) = send_message(consumer, frames).await {
                        has_sent = true;
                        break;
                    }
                }

                if !has_sent {
                    messages.push_back(message);
                }
            }
            QueueCommand::Consume {
                consumer_tag,
                frame_sink,
                response,
            } => {
                info!("Basic Consume {}", consumer_tag);

                consumers.insert(consumer_tag, frame_sink);

                if let Err(e) = response.send(()) {
                    error!("Send error {:?}", e);
                }

                // start sending pending messages
            }
            QueueCommand::Cancel { consumer_tag, response } => {
                info!("Basic Cancel {}", consumer_tag);

                consumers.remove(&consumer_tag);

                if let Err(e) = response.send(()) {
                    error!("Send error {:?}", e);
                }
            }
        }
    }
}

/// Send the message as frames to a consumer. Returns true if all the frames managed to
/// be sent to the channel. The caller of this function should take care of the result,
/// and in case of a failed sending, it should try to send to another consumer, or
/// if there is no consumer, it should store the message.
async fn send_message(consumer: &FrameSink, frames: Vec<frame::AMQPFrame>) -> Result<bool> {
    let mut n: usize = 0;

    'frames: for f in &frames {
        debug!("Sending frame {:?}", f);

        if let Err(e) = consumer.send(f.clone()).await {
            // TODO remove this channel from the consumers
            error!("Message send error {:?}", e);
            break 'frames;
        } else {
            n += 1
        }
    }

    Ok(n == frames.len())
}

fn message_to_frames(message: &Message, consumer_tag: &ConsumerTag) -> Vec<frame::AMQPFrame> {
    vec![
        frame::basic_deliver(
            message.channel,
            consumer_tag,
            0,
            false,
            &message.exchange,
            &message.routing_key,
        ),
        frame::AMQPFrame::ContentHeader(frame::content_header(1, message.content.len() as u64)),
        frame::AMQPFrame::ContentBody(frame::content_body(1, message.content.as_slice())),
    ]
}
