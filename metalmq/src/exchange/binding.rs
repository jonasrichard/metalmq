use crate::{
    logerr,
    message::Message,
    queue::handler::{QueueCommand, QueueCommandSink},
    Result,
};
use log::{debug, error};
use metalmq_codec::frame::{self, AMQPFieldValue, FieldTable};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::oneshot;

use super::ExchangeType;

#[derive(Debug)]
pub struct DirectBinding {
    pub routing_key: String,
    pub queue_name: String,
    pub queue: QueueCommandSink,
}

#[derive(Debug)]
pub struct FanoutBinding {
    pub queue_name: String,
    pub queue: QueueCommandSink,
}

#[derive(Debug)]
pub struct TopicBinding {
    pub routing_key: String,
    pub queue_name: String,
    pub queue: QueueCommandSink,
}

#[derive(Debug)]
pub struct HeadersBinding {
    pub headers: HashMap<String, AMQPFieldValue>,
    pub x_match_all: bool,
    pub queue_name: String,
    pub queue: QueueCommandSink,
}

/// Represents exchange-queue binding. In one binding the different binding types should be of the
/// same type. The Bindings type will keep those invariants during adding new binding, removing old
/// ones.
#[derive(Debug)]
pub enum Bindings {
    Direct(Vec<DirectBinding>),
    Fanout(Vec<FanoutBinding>),
    Topic(Vec<TopicBinding>),
    Headers(Vec<HeadersBinding>),
}

impl Bindings {
    pub fn new(exchange_type: ExchangeType) -> Self {
        match exchange_type {
            ExchangeType::Direct => Bindings::Direct(vec![]),
            ExchangeType::Fanout => Bindings::Fanout(vec![]),
            ExchangeType::Topic => Bindings::Topic(vec![]),
            ExchangeType::Headers => Bindings::Headers(vec![]),
        }
    }

    /// Add a new direct binding to the binding list. If that queue with the given routing key is
    /// already bound, it returns `false`.
    pub fn add_direct_binding(&mut self, routing_key: String, queue_name: String, queue: QueueCommandSink) -> bool {
        if let Bindings::Direct(bs) = self {
            if bs
                .iter()
                .any(|b| b.routing_key == routing_key && b.queue_name == queue_name)
            {
                // routing key and queue name are already bound
                return false;
            }

            bs.push(DirectBinding {
                routing_key,
                queue_name,
                queue,
            });

            return true;
        }

        false
    }

    pub fn remove_direct_binding(&mut self, routing_key: &str, queue_name: &str) -> Option<QueueCommandSink> {
        if let Bindings::Direct(bs) = self {
            if let Some(p) = bs
                .iter()
                .position(|b| b.routing_key == routing_key && b.queue_name == queue_name)
            {
                let binding = bs.remove(p);

                return Some(binding.queue);
            }
        }

        None
    }

    pub fn add_topic_binding(&mut self, routing_key: String, queue_name: String, queue: QueueCommandSink) -> bool {
        if let Bindings::Topic(bs) = self {
            if bs
                .iter()
                .any(|b| b.routing_key == routing_key && b.queue_name == queue_name)
            {
                // routing key and queue name are already bound
                return false;
            }

            bs.push(TopicBinding {
                routing_key,
                queue_name,
                queue,
            });

            return true;
        }

        false
    }

    pub fn remove_topic_binding(&mut self, routing_key: &str, queue_name: &str) -> Option<QueueCommandSink> {
        if let Bindings::Topic(bs) = self {
            if let Some(p) = bs
                .iter()
                .position(|b| b.routing_key == routing_key && b.queue_name == queue_name)
            {
                let binding = bs.remove(p);

                return Some(binding.queue);
            }
        }

        None
    }

    pub fn add_fanout_binding(&mut self, queue_name: String, queue: QueueCommandSink) -> bool {
        if let Bindings::Fanout(bs) = self {
            if bs.iter().any(|b| b.queue_name == queue_name) {
                return false;
            }

            bs.push(FanoutBinding { queue_name, queue });

            return true;
        }

        false
    }

    pub fn remove_fanout_binding(&mut self, queue_name: &str) -> Option<QueueCommandSink> {
        if let Bindings::Fanout(bs) = self {
            if let Some(p) = bs.iter().position(|b| b.queue_name == queue_name) {
                let binding = bs.remove(p);

                return Some(binding.queue);
            }
        }

        None
    }

    pub fn add_headers_binding(
        &mut self,
        queue_name: String,
        args: Option<frame::FieldTable>,
        queue: QueueCommandSink,
    ) -> bool {
        // TODO here we can send back error, if there are no headers
        if args.is_none() {
            return false;
        }

        if let Bindings::Headers(bs) = self {
            // TODO check if there is another binding with the exact same headers
            let hb = header_binding_from_field_table(args.unwrap(), queue_name, queue);

            bs.push(hb);

            return true;
        }

        false
    }

    pub fn remove_queue(&mut self, queue_name: &str) {
        match self {
            Bindings::Direct(bs) => {
                bs.retain(|b| b.queue_name != queue_name);
            }
            Bindings::Fanout(bs) => {
                bs.retain(|b| b.queue_name != queue_name);
            }
            Bindings::Topic(bs) => {
                bs.retain(|b| b.queue_name != queue_name);
            }
            Bindings::Headers(bs) => {
                bs.retain(|b| b.queue_name != queue_name);
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Bindings::Direct(bs) => bs.is_empty(),
            Bindings::Topic(bs) => bs.is_empty(),
            Bindings::Fanout(bs) => bs.is_empty(),
            Bindings::Headers(bs) => bs.is_empty(),
        }
    }

    /// Route the message according to the exchange type and the bindings. If there is no queue to
    /// be send the message to, it gives back the messages in the Option.
    pub async fn route_message(&self, message: Message) -> Result<Option<Arc<Message>>> {
        let mut sent = false;
        let shared_message = Arc::new(message);

        dbg!(self);

        match self {
            Bindings::Direct(bs) => {
                for binding in bs {
                    if binding.routing_key == shared_message.routing_key {
                        debug!("Routing message to {}", binding.queue_name);

                        logerr!(
                            binding
                                .queue
                                .send(QueueCommand::PublishMessage(shared_message.clone()))
                                .await
                        );
                        sent = true;
                    }
                }
            }
            Bindings::Fanout(bs) => {
                for binding in bs {
                    debug!("Routing message to {}", binding.queue_name);

                    logerr!(
                        binding
                            .queue
                            .send(QueueCommand::PublishMessage(shared_message.clone()))
                            .await
                    );
                    sent = true;
                }
            }
            Bindings::Topic(bs) => {
                for binding in bs {
                    if match_routing_key(&binding.routing_key, &shared_message.routing_key) {
                        debug!("Routing message to {}", binding.queue_name);

                        logerr!(
                            binding
                                .queue
                                .send(QueueCommand::PublishMessage(shared_message.clone()))
                                .await
                        );
                        sent = true;
                    }
                }
            }
            Bindings::Headers(bs) => {
                if let Some(ref headers) = shared_message.content.headers {
                    for binding in bs {
                        if match_header(&binding.headers, headers, binding.x_match_all) {
                            debug!("Routing message to {}", binding.queue_name);

                            logerr!(
                                binding
                                    .queue
                                    .send(QueueCommand::PublishMessage(shared_message.clone()))
                                    .await
                            );
                            sent = true;
                        }
                    }
                }
            }
        }

        if sent {
            Ok(None)
        } else {
            Ok(Some(shared_message))
        }
    }

    async fn send_exchange_unbound(&self, cmd_sink: &QueueCommandSink, exchange_name: &str) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let cmd = QueueCommand::ExchangeUnbound {
            exchange_name: exchange_name.to_string(),
            result: tx,
        };
        cmd_sink.send(cmd).await?;
        rx.await?
    }

    pub async fn broadcast_exchange_unbound(&self, exchange_name: &str) -> Result<()> {
        match self {
            Bindings::Direct(bs) => {
                for b in bs {
                    self.send_exchange_unbound(&b.queue, exchange_name).await?;
                }
            }
            Bindings::Fanout(bs) => {
                for b in bs {
                    self.send_exchange_unbound(&b.queue, exchange_name).await?;
                }
            }
            Bindings::Topic(bs) => {
                for b in bs {
                    self.send_exchange_unbound(&b.queue, exchange_name).await?;
                }
            }
            Bindings::Headers(bs) => {
                for b in bs {
                    self.send_exchange_unbound(&b.queue, exchange_name).await?;
                }
            }
        }

        Ok(())
    }
}

fn header_binding_from_field_table(ft: FieldTable, queue_name: String, queue: QueueCommandSink) -> HeadersBinding {
    let mut headers = HashMap::new();
    let mut x_match_all = true;

    for (ftk, ftv) in ft {
        // Ignore all headers which starts with "x-"
        if ftk.starts_with("x-") {
            if ftk == "x-match" {
                match ftv {
                    AMQPFieldValue::LongString(s) if s == "any" => x_match_all = false,
                    _ => (),
                }
            }

            continue;
        }

        headers.insert(ftk, ftv);
    }

    HeadersBinding {
        headers,
        x_match_all,
        queue_name,
        queue,
    }
}

pub fn match_routing_key(binding_key: &str, message_routing_key: &str) -> bool {
    let mut bks: Vec<_> = binding_key.split('.').collect();
    let mks: Vec<_> = message_routing_key.split('.').collect();

    // empty routing key?

    for message_key in mks {
        if bks.is_empty() {
            return false;
        }

        let b_key = bks.remove(0);

        match b_key {
            "*" => continue,
            "#" => return true,
            _ if b_key == message_key => continue,
            _ => return false,
        }
    }

    bks.is_empty()
}

pub fn match_header(
    binding_headers: &HashMap<String, AMQPFieldValue>,
    message_headers: &FieldTable,
    x_match_all: bool,
) -> bool {
    let mut matches = 0usize;

    debug!("Binding headers {binding_headers:?} message headers {message_headers:?}");

    for (bhk, bhv) in binding_headers {
        if let Some(mhv) = message_headers.get(bhk) {
            if *bhv == *mhv {
                matches += 1;
            }
        }
    }

    if x_match_all {
        matches == binding_headers.len()
    } else {
        matches > 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    #[test]
    fn test_match_routing_key() {
        assert!(match_routing_key("stocks.nwse.goog", "stocks.nwse.goog"));
        assert!(match_routing_key("stocks.*.goog", "stocks.nwse.goog"));
        assert!(match_routing_key("stocks.nwse.*", "stocks.nwse.goog"));
        assert!(match_routing_key("stocks.*.*", "stocks.nwse.goog"));
        assert!(match_routing_key("stocks.#", "stocks.nwse.goog"));
    }

    #[tokio::test]
    async fn test_direct_binding() {
        let (tx, mut rx) = mpsc::channel(1);

        let mut bindings = Bindings::new(ExchangeType::Direct);
        bindings.add_direct_binding("extension.png".to_string(), "png-images".to_string(), tx);

        let mut message = Message::default();
        message.exchange = "images".to_string();
        message.routing_key = "extension.png".to_string();

        let result = bindings.route_message(message).await.unwrap();

        // The message is not returned but successfully routed.
        assert!(result.is_none());

        let delivered = rx.recv().await.unwrap();
        assert!(matches!(delivered, QueueCommand::PublishMessage(_)));
    }
}
