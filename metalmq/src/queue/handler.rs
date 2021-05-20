use crate::message::Message;
use crate::ConsumerTag;
use log::{debug, error};
use metalmq_codec::frame;
use std::collections::HashMap;
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
    let mut consumers = HashMap::<ConsumerTag, FrameSink>::new();

    while let Some(command) = commands.recv().await {
        match command {
            QueueCommand::Message(message) => {
                debug!("Sending message to consumers {}", consumers.len());

                'consumer: for (consumer_tag, consumer) in &consumers {
                    let frames = vec![
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
                    ];

                    'frames: for f in &frames {
                        debug!("Sending frame {:?}", f);

                        if let Err(e) = consumer.send(f.clone()).await {
                            error!("Message send error {:?}", e);
                            break 'frames;
                        }
                    }
                }
            }
            QueueCommand::Consume {
                consumer_tag,
                frame_sink,
                response,
            } => {
                debug!("Basic Consume {}", consumer_tag);

                consumers.insert(consumer_tag, frame_sink);

                if let Err(e) = response.send(()) {
                    error!("Send error {:?}", e);
                }
            }
            QueueCommand::Cancel { consumer_tag, response } => {
                debug!("Basic Cancel {}", consumer_tag);

                consumers.remove(&consumer_tag);

                if let Err(e) = response.send(()) {
                    error!("Send error {:?}", e);
                }
            }
        }
    }
}
