use crate::client::state::{Connection, MaybeFrame};
use crate::client::{self, ChannelError};
use crate::exchange::manager::{self as em, BindQueueCommand, UnbindQueueCommand};
use crate::queue::manager as qm;
use metalmq_codec::codec::Frame;
use metalmq_codec::frame::{self, Channel};

impl Connection {
    pub(crate) async fn queue_declare(&mut self, channel: Channel, args: frame::QueueDeclareArgs) -> MaybeFrame {
        let queue_name = args.name.clone();

        qm::declare_queue(&self.qm, args.into(), &self.id, channel).await?;

        Ok(Some(Frame::Frame(frame::queue_declare_ok(channel, queue_name, 0, 0))))
    }

    pub(crate) async fn queue_bind(&mut self, channel: Channel, args: frame::QueueBindArgs) -> MaybeFrame {
        match qm::get_command_sink(&self.qm, channel, &args.queue_name).await {
            Ok(sink) => {
                let cmd = BindQueueCommand {
                    channel,
                    exchange_name: args.exchange_name,
                    queue_name: args.queue_name,
                    routing_key: args.routing_key,
                    queue_sink: sink,
                };

                em::bind_queue(&self.em, cmd).await?;

                Ok(Some(Frame::Frame(frame::queue_bind_ok(channel))))
            }
            Err(_) => client::channel_error(channel, frame::QUEUE_BIND, ChannelError::NotFound, "Exchange not found"),
        }
    }

    pub(crate) async fn queue_delete(&mut self, channel: Channel, args: frame::QueueDeleteArgs) -> MaybeFrame {
        // TODO delete the queue
        Ok(Some(Frame::Frame(frame::queue_delete_ok(channel, 0))))
    }

    pub(crate) async fn queue_unbind(&mut self, channel: Channel, args: frame::QueueUnbindArgs) -> MaybeFrame {
        let cmd = UnbindQueueCommand {
            channel,
            exchange_name: args.exchange_name,
            queue_name: args.queue_name,
            routing_key: args.routing_key,
        };

        em::unbind_queue(&self.em, cmd).await?;

        Ok(Some(Frame::Frame(frame::queue_unbind_ok(channel))))
    }
}
