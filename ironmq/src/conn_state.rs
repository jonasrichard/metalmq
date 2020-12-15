use crate::Result;
use ironmq_codec::frame;
use ironmq_codec::frame::{AMQPFrame, Channel};
use log::info;
use std::collections::HashMap;

pub(crate) type MaybeFrame = Result<Option<AMQPFrame>>;

const NOT_FOUND: u16 = 404;
const CHANNEL_ERROR: u16 = 504;
const NOT_ALLOWED: u16 = 530;

// TODO handling exchanges globally
//
pub(crate) struct ConnectionState {
    virtual_host: String,
    open_channels: HashMap<Channel, ()>,
    exchanges: HashMap<String, ()>,
    queues: HashMap<String, ()>,
    /// Simple exchange-queue binding
    binding: HashMap<(String, String), ()>
}

pub(crate) trait Connection {
    fn connection_open(&mut self, channel: Channel, args: frame::ConnectionOpenArgs) -> MaybeFrame;
    fn connection_close(&mut self, args: frame::ConnectionCloseArgs) -> MaybeFrame;
    fn channel_open(&mut self, channel: Channel) -> MaybeFrame;
    fn channel_close(&mut self, channel: Channel, args: frame::ChannelCloseArgs) -> MaybeFrame;
    fn exchange_declare(&mut self, channel: Channel, args: frame::ExchangeDeclareArgs) -> MaybeFrame;
    fn queue_declare(&mut self, channel: Channel, args: frame::QueueDeclareArgs) -> MaybeFrame;
    fn queue_bind(&mut self, channel: Channel, args: frame::QueueBindArgs) -> MaybeFrame;
    fn basic_publish(&mut self, channel: Channel, args: frame::BasicPublishArgs) -> MaybeFrame;
    fn receive_content_header(&mut self, header: frame::ContentHeaderFrame) -> MaybeFrame;
    fn receive_content_body(&mut self, header: frame::ContentBodyFrame) -> MaybeFrame;
}

impl Default for ConnectionState {
    fn default() -> Self {
        ConnectionState {
            virtual_host: "".into(),
            open_channels: HashMap::new(),
            exchanges: HashMap::new(),
            queues: HashMap::new(),
            binding: HashMap::new()
        }
    }
}

impl Connection for ConnectionState {
    fn connection_open(&mut self, channel: Channel, args: frame::ConnectionOpenArgs) -> MaybeFrame {
        // TODO check if virtual host doens't exist
        self.virtual_host = args.virtual_host;
        Ok(Some(frame::connection_open_ok(channel)))
    }

    fn connection_close(&mut self, args: frame::ConnectionCloseArgs) -> MaybeFrame {
        // TODO cleanup
        Ok(Some(frame::connection_close_ok(0)))
    }

    fn channel_open(&mut self, channel: Channel) -> MaybeFrame {
        if self.open_channels.contains_key(&channel) {
            let (cid, mid) = frame::split_class_method(frame::CHANNEL_OPEN);
            Ok(Some(frame::channel_close(channel, CHANNEL_ERROR, "Channel already opened", cid, mid)))
        } else {
            self.open_channels.insert(channel, ());
            Ok(Some(frame::channel_open_ok(channel)))
        }
    }

    fn channel_close(&mut self, channel: Channel, args: frame::ChannelCloseArgs) -> MaybeFrame {
        self.open_channels.remove(&channel);
        Ok(Some(frame::channel_close_ok(channel)))
    }

    fn exchange_declare(&mut self, channel: Channel, args: frame::ExchangeDeclareArgs) -> MaybeFrame {
        if self.exchanges.contains_key(&args.exchange_name) {
            let (cid, mid) = frame::split_class_method(frame::EXCHANGE_DECLARE);
            Ok(Some(frame::channel_close(channel, NOT_ALLOWED, "Exchange already exists", cid, mid)))
        } else {
            self.exchanges.insert(args.exchange_name, ());
            Ok(Some(frame::exchange_declare_ok(channel)))
        }
    }

    fn queue_declare(&mut self, channel: Channel, args: frame::QueueDeclareArgs) -> MaybeFrame {
        if !self.queues.contains_key(&args.name) {
            self.queues.insert(args.name.clone(), ());
        }

        Ok(Some(frame::queue_declare_ok(channel, args.name, 0, 0)))
    }

    fn queue_bind(&mut self, channel: Channel, args: frame::QueueBindArgs) -> MaybeFrame {
        let binding = (args.exchange_name, args.queue_name);

        if !self.binding.contains_key(&binding) {
            self.binding.insert(binding, ());
        }

        Ok(Some(frame::queue_bind_ok(channel)))
    }

    fn basic_publish(&mut self, channel: Channel, args: frame::BasicPublishArgs) -> MaybeFrame {
        if !self.exchanges.contains_key(&args.exchange_name) {
            let (cid, mid) = frame::split_class_method(frame::BASIC_PUBLISH);
            Ok(Some(frame::channel_close(channel, NOT_FOUND, "Exchange not found", cid, mid)))
        } else {
            Ok(None)
        }
    }

    fn receive_content_header(&mut self, header: frame::ContentHeaderFrame) -> MaybeFrame {
        // TODO collect info into a data struct
        info!("Receive content with length {}", header.body_size);
        Ok(None)
    }

    fn receive_content_body(&mut self, body: frame::ContentBodyFrame) -> MaybeFrame {
        info!("Receive content with length {}", body.body.len());
        Ok(None)
    }
}
