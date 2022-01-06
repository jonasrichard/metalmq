use crate::client::state::Connection;
use crate::client::{self, ChannelError};
use crate::exchange::manager::{self as em, BindQueueCommand, UnbindQueueCommand};
use crate::queue::manager as qm;
use crate::Result;
use metalmq_codec::codec::Frame;
use metalmq_codec::frame::{self, Channel};

impl Connection {
    pub async fn queue_declare(&mut self, channel: Channel, args: frame::QueueDeclareArgs) -> Result<()> {
        let queue_name = args.name.clone();

        let cmd = qm::QueueDeclareCommand {
            conn_id: self.id.clone(),
            channel,
            queue: args.into(),
        };
        qm::declare_queue(&self.qm, cmd).await?;

        self.send_frame(Frame::Frame(frame::queue_declare_ok(channel, queue_name, 0, 0)))
            .await?;

        Ok(())
    }

    pub async fn queue_bind(&mut self, channel: Channel, args: frame::QueueBindArgs) -> Result<()> {
        let cmd = qm::GetQueueSinkQuery {
            channel,
            queue_name: args.queue_name.clone(),
        };

        match qm::get_command_sink(&self.qm, cmd).await {
            Ok(sink) => {
                let cmd = BindQueueCommand {
                    channel,
                    exchange_name: args.exchange_name,
                    queue_name: args.queue_name,
                    routing_key: args.routing_key,
                    args: args.args,
                    queue_sink: sink,
                };

                em::bind_queue(&self.em, cmd).await?;

                self.send_frame(Frame::Frame(frame::queue_bind_ok(channel))).await?;
            }
            Err(_) => {
                self.send_frame(client::channel_error_frame(
                    channel,
                    frame::QUEUE_BIND,
                    ChannelError::NotFound,
                    "Exchange not found",
                ))
                .await?;
            }
        }

        Ok(())
    }

    pub async fn queue_delete(&mut self, channel: Channel, args: frame::QueueDeleteArgs) -> Result<()> {
        // TODO delete the queue
        self.send_frame(Frame::Frame(frame::queue_delete_ok(channel, 0)))
            .await?;

        Ok(())
    }

    pub async fn queue_unbind(&mut self, channel: Channel, args: frame::QueueUnbindArgs) -> Result<()> {
        let cmd = UnbindQueueCommand {
            channel,
            exchange_name: args.exchange_name,
            queue_name: args.queue_name,
            routing_key: args.routing_key,
        };

        em::unbind_queue(&self.em, cmd).await?;

        self.send_frame(Frame::Frame(frame::queue_unbind_ok(channel))).await?;

        Ok(())
    }
}
