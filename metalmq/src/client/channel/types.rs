use std::collections::HashMap;

use crate::error::Result;
use crate::exchange;
use crate::queue;

use metalmq_codec::codec::Frame;
use metalmq_codec::frame::{AMQPFrame, ContentBodyFrame, ContentHeaderFrame};

use tokio::sync::mpsc;

/// Queues consumed by the connection with Basic.Consume
#[derive(Debug)]
pub struct ActivelyConsumedQueue {
    pub queue_name: String,
    pub consumer_tag: String,
    pub queue_sink: queue::handler::QueueCommandSink,
}

/// Queues consumed by the connection with Basic.Get
#[derive(Debug)]
pub struct PassivelyConsumedQueue {
    pub queue_name: String,
    pub consumer_tag: String,
    pub delivery_tag: u64,
    pub queue_sink: queue::handler::QueueCommandSink,
}

#[derive(Debug, Default)]
pub struct PublishedContent {
    pub source_connection: String,
    pub channel: u16,
    pub exchange: String,
    pub routing_key: String,
    pub mandatory: bool,
    pub immediate: bool,
    /// The method frame class id which initiated the sending of the content.
    pub method_frame_class_id: u32,
    pub content_header: ContentHeaderFrame,
    pub content_bodies: Vec<ContentBodyFrame>,
    pub body_size: usize,
}

/// Represents a channel
#[derive(Debug)]
pub struct Channel {
    pub source_connection: String,
    pub number: u16,
    pub consumed_queue: Option<ActivelyConsumedQueue>,
    pub in_flight_content: Option<PublishedContent>,
    pub confirm_mode: bool,
    pub next_confirm_delivery_tag: u64,
    pub outgoing: mpsc::Sender<Frame>,
    pub exchanges: HashMap<String, exchange::handler::ExchangeCommandSink>,
    pub em: exchange::manager::ExchangeManagerSink,
    pub qm: queue::manager::QueueManagerSink,
}

impl Channel {
    pub async fn handle_message(&mut self, mut rx: mpsc::Receiver<AMQPFrame>) -> Result<()> {
        while let Some(f) = rx.recv().await {
            match f {
                AMQPFrame::Method(ch, cm, ma) => {
                    let result = match ma {
                        metalmq_codec::frame::MethodFrameArgs::ChannelClose(_) => self.handle_channel_close(ch).await,
                        metalmq_codec::frame::MethodFrameArgs::ChannelCloseOk => todo!(),
                        metalmq_codec::frame::MethodFrameArgs::ExchangeDeclare(args) => {
                            self.handle_exchange_declare(args).await
                        }
                        metalmq_codec::frame::MethodFrameArgs::ExchangeDeclareOk => todo!(),
                        metalmq_codec::frame::MethodFrameArgs::ExchangeDelete(_) => todo!(),
                        metalmq_codec::frame::MethodFrameArgs::ExchangeDeleteOk => todo!(),
                        metalmq_codec::frame::MethodFrameArgs::QueueDeclare(_) => todo!(),
                        metalmq_codec::frame::MethodFrameArgs::QueueDeclareOk(_) => todo!(),
                        metalmq_codec::frame::MethodFrameArgs::QueueBind(_) => todo!(),
                        metalmq_codec::frame::MethodFrameArgs::QueueBindOk => todo!(),
                        metalmq_codec::frame::MethodFrameArgs::QueuePurge(_) => todo!(),
                        metalmq_codec::frame::MethodFrameArgs::QueuePurgeOk(_) => todo!(),
                        metalmq_codec::frame::MethodFrameArgs::QueueDelete(_) => todo!(),
                        metalmq_codec::frame::MethodFrameArgs::QueueDeleteOk(_) => todo!(),
                        metalmq_codec::frame::MethodFrameArgs::QueueUnbind(_) => todo!(),
                        metalmq_codec::frame::MethodFrameArgs::QueueUnbindOk => todo!(),
                        metalmq_codec::frame::MethodFrameArgs::BasicConsume(_) => todo!(),
                        metalmq_codec::frame::MethodFrameArgs::BasicConsumeOk(_) => todo!(),
                        metalmq_codec::frame::MethodFrameArgs::BasicCancel(_) => todo!(),
                        metalmq_codec::frame::MethodFrameArgs::BasicCancelOk(_) => todo!(),
                        metalmq_codec::frame::MethodFrameArgs::BasicGet(_) => todo!(),
                        metalmq_codec::frame::MethodFrameArgs::BasicGetOk(_) => todo!(),
                        metalmq_codec::frame::MethodFrameArgs::BasicGetEmpty => todo!(),
                        metalmq_codec::frame::MethodFrameArgs::BasicPublish(_) => todo!(),
                        metalmq_codec::frame::MethodFrameArgs::BasicReturn(_) => todo!(),
                        metalmq_codec::frame::MethodFrameArgs::BasicDeliver(_) => todo!(),
                        metalmq_codec::frame::MethodFrameArgs::BasicAck(_) => todo!(),
                        metalmq_codec::frame::MethodFrameArgs::BasicReject(_) => todo!(),
                        metalmq_codec::frame::MethodFrameArgs::BasicNack(_) => todo!(),
                        metalmq_codec::frame::MethodFrameArgs::ConfirmSelect(_) => todo!(),
                        metalmq_codec::frame::MethodFrameArgs::ConfirmSelectOk => todo!(),
                        _ => unreachable!(),
                    };
                }
                AMQPFrame::ContentHeader(_) => todo!(),
                AMQPFrame::ContentBody(_) => todo!(),
                _ => unreachable!(),
            }
        }

        Ok(())
    }
}
