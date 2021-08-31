use crate::exchange::Exchange;
use crate::message::{self, Message};
use crate::queue::handler::{QueueCommand, QueueCommandSink};
use crate::{logerr, send, Result};
use log::{error, info, trace};
use metalmq_codec::codec::Frame;
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

pub(crate) type ExchangeCommandSink = mpsc::Sender<ExchangeCommand>;

#[derive(Debug)]
pub(crate) enum MessageSentResult {
    None,
    MessageNotRouted(Message),
}

#[derive(Debug)]
pub(crate) enum ExchangeCommand {
    Message(Message),
    QueueBind {
        queue_name: String,
        routing_key: String,
        sink: QueueCommandSink,
        result: oneshot::Sender<bool>,
    },
    QueueUnbind {
        queue_name: String,
        routing_key: String,
        result: oneshot::Sender<bool>,
    },
}

struct ExchangeState {
    exchange: super::Exchange,
    /// Queues bound by routing key
    queues: HashMap<String, QueueCommandSink>,
    outgoing: mpsc::Sender<Frame>,
}

pub(crate) async fn start(
    exchange: Exchange,
    commands: &mut mpsc::Receiver<ExchangeCommand>,
    outgoing: mpsc::Sender<Frame>,
) {
    ExchangeState {
        exchange,
        queues: HashMap::new(),
        outgoing,
    }
    .exchange_loop(commands)
    .await;
}

impl ExchangeState {
    pub(crate) async fn exchange_loop(&mut self, commands: &mut mpsc::Receiver<ExchangeCommand>) -> Result<()> {
        while let Some(command) = commands.recv().await {
            trace!("Command {:?}", command);

            self.handle_command(command).await;
        }

        Ok(())
    }

    pub(crate) async fn handle_command(&mut self, command: ExchangeCommand) -> Result<()> {
        match command {
            ExchangeCommand::Message(message) => {
                match self.choose_queue_by_routing_key(&message.routing_key) {
                    Some(queue) => {
                        // TODO here we need to check if this exchange is bound to a queue, or
                        // if routing key will send this message to a queue.
                        //   If not, we need to check if the message is mandatory, we need to
                        //   send back a basic-return with an error.
                        info!(
                            "Publish message {}",
                            String::from_utf8(message.content.clone()).unwrap()
                        );

                        // FIXME this causes deadlock
                        if let Err(e) = send!(queue, QueueCommand::PublishMessage(message)) {
                            error!("Send error {:?}", e);
                        }
                    }
                    None => {
                        if message.mandatory {
                            message::send_basic_return(&message, &self.outgoing).await?;
                        }
                    }
                }
            }
            ExchangeCommand::QueueBind {
                queue_name,
                routing_key,
                sink,
                result,
            } => {
                self.queues.insert(routing_key, sink.clone());
                logerr!(send!(
                    sink,
                    QueueCommand::ExchangeBound {
                        exchange_name: self.exchange.name.clone(),
                    }
                ));
                logerr!(result.send(true));
            }
            ExchangeCommand::QueueUnbind {
                queue_name,
                routing_key,
                result,
            } => {
                if let Some(sink) = self.queues.remove(&routing_key) {
                    logerr!(send!(
                        sink,
                        QueueCommand::ExchangeUnbound {
                            exchange_name: self.exchange.name.clone(),
                        }
                    ));
                }

                logerr!(result.send(true));
            }
        }

        Ok(())
    }

    fn choose_queue_by_routing_key(&self, routing_key: &str) -> Option<&QueueCommandSink> {
        self.queues.get(routing_key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metalmq_codec::frame;

    async fn recv_timeout(rx: &mut mpsc::Receiver<Frame>) -> Option<Frame> {
        let sleep = tokio::time::sleep(tokio::time::Duration::from_secs(1));
        tokio::pin!(sleep);

        tokio::select! {
            frame = rx.recv() => {
                return frame;
            }
            _ = &mut sleep => {
                return None;
            }
        }
    }

    #[tokio::test]
    async fn send_basic_return_on_mandatory_unroutable_message() {
        let (msg_tx, mut msg_rx) = mpsc::channel(1);
        let mut es = ExchangeState {
            exchange: Exchange {
                name: "x-name".to_string(),
                exchange_type: "direct".to_string(),
                durable: false,
                auto_delete: false,
                internal: false,
            },
            queues: HashMap::new(),
            outgoing: msg_tx,
        };

        let msg = Message {
            source_connection: "conn-id".to_string(),
            channel: 2,
            content: b"Hello".to_vec(),
            exchange: "x-name".to_string(),
            routing_key: "".to_string(),
            mandatory: true,
            immediate: false,
        };
        let cmd = ExchangeCommand::Message(msg);

        let res = es.handle_command(cmd).await;
        assert!(res.is_ok());

        match recv_timeout(&mut msg_rx).await {
            Some(Frame::Frame(br)) => assert!(true),
            Some(Frame::Frames(fs)) => if let frame::AMQPFrame::Method(ch, cm, args) = fs.get(0).unwrap() {},
            None => assert!(false, "Basic.Return frame is expected"),
        }
    }
}
