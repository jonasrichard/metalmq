use crate::Result;
use crate::message::Message;
use ironmq_codec::frame;
use log::{debug, error};
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

pub(crate) type ControlChannel = mpsc::Sender<ManagerCommand>;
pub(crate) type QueueChannel = mpsc::Sender<QueueCommand>;
pub(crate) type FrameChannel = mpsc::Sender<frame::AMQPFrame>;

#[derive(Debug)]
pub(crate) enum ManagerCommand {
    QueueClone { name: String, clone: oneshot::Sender<QueueChannel> },
    Consume { queue_name: String, sink: FrameChannel, success: oneshot::Sender<()> }
}

#[derive(Debug)]
pub(crate) enum QueueCommand {
    Message(Message),
    Consume{ sink: FrameChannel }
}

pub(crate) async fn queue_manager_loop(control: &mut mpsc::Receiver<ManagerCommand>) -> Result<()> {
    let mut queues = HashMap::<String, QueueChannel>::new();

    while let Some(command) = control.recv().await {
        debug!("{:?}", command);

        match command {
            ManagerCommand::QueueClone{ name, clone } => {
                if let Some(queue) = queues.get(&name) {
                    if let Err(e) = clone.send(queue.clone()) {
                        error!("Send error {:?}", e);

                        return Ok(());
                    }
                } else {
                    let (tx, mut rx) = mpsc::channel(1);

                    tokio::spawn(async move {
                        queue_loop(&mut rx).await;
                    });

                    let result = tx.clone();
                    if let Err(e) = clone.send(tx) {
                        error!("Send error {:?}", e);

                        return Ok(());
                    }

                    queues.insert(name, result);
                }
            },
            ManagerCommand::Consume{ queue_name, sink, success } => {
                if let Some(queue) = queues.get(&queue_name) {
                    // TODO we need to send proper error in case of unsuccessful consume subscribe
                    if let Err(e) = success.send(()) {
                        error!("Send error {:?}", e);

                        return Ok(());
                    }

                    queue.send(QueueCommand::Consume{ sink: sink }).await?;
                }
            }
        }
    }

    Ok(())
}

pub(crate) async fn queue_loop(commands: &mut mpsc::Receiver<QueueCommand>) {
    let mut consumers = Vec::<FrameChannel>::new();

    while let Some(command) = commands.recv().await {
        debug!("Queue got message: {:?}", command);

        match command {
            QueueCommand::Message(message) => {
                let frames = vec![
                    frame::basic_deliver(1, "ctag".into(), 0, false, "exchange".into(), "rkey".into()),
                    frame::AMQPFrame::ContentHeader(frame::content_header(1, message.content.len() as u64)),
                    frame::AMQPFrame::ContentBody(frame::content_body(1, message.content.as_slice())),
                ];

                if let Some(c) = consumers.get(0) {
                    for f in frames {
                        if let Err(e) = c.send(f).await {
                            error!("Message send error {:?}", e);
                        }
                    }
                }
                //for ch in &consumers {
                //    for f in &frames {
                //        ch.send(f.clone()).await;
                //    }
                //}
            },
            QueueCommand::Consume{ sink } =>
                consumers.push(sink)
        }
    }
}
