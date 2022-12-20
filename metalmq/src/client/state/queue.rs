use crate::client::state::Connection;
use crate::client::{self, ChannelError};
use crate::exchange::manager::{self as em, BindQueueCommand, UnbindQueueCommand};
use crate::queue::manager as qm;
use crate::{handle_error, Result};
use log::{info, warn};
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
                    conn_id: self.id.clone(),
                    channel,
                    exchange_name: args.exchange_name,
                    queue_name: args.queue_name,
                    routing_key: args.routing_key,
                    args: args.args,
                    queue_sink: sink,
                };

                handle_error!(self, em::bind_queue(&self.em, cmd).await).unwrap();

                self.send_frame(Frame::Frame(frame::queue_bind_ok(channel))).await?;
            }
            Err(e) => {
                warn!("{:?}", e);

                handle_error!(
                    self,
                    client::channel_error::<()>(channel, frame::QUEUE_BIND, ChannelError::NotFound, "Queue not found",)
                )
                .unwrap();
                //handle_error!(
                //    self,
                //    client::channel_error::<()>(channel, frame::QUEUE_BIND, ChannelError::NotFound, "Queue not found",)
                //)
                //.unwrap();

                info!("Survived error handling");
            }
        }

        Ok(())
    }

    pub async fn queue_delete(&mut self, channel: Channel, args: frame::QueueDeleteArgs) -> Result<()> {
        let cmd = qm::QueueDeleteCommand {
            conn_id: self.id.clone(),
            channel,
            queue_name: args.queue_name,
            if_unused: args.flags.contains(frame::QueueDeleteFlags::IF_UNUSED),
            if_empty: args.flags.contains(frame::QueueDeleteFlags::IF_EMPTY),
        };

        let message_count = handle_error!(self, qm::delete_queue(&self.qm, cmd).await).unwrap();

        self.send_frame(Frame::Frame(frame::queue_delete_ok(channel, message_count)))
            .await?;

        Ok(())
    }

    pub async fn queue_unbind(&mut self, channel: Channel, args: frame::QueueUnbindArgs) -> Result<()> {
        let cmd = UnbindQueueCommand {
            conn_id: self.id.clone(),
            channel,
            exchange_name: args.exchange_name,
            queue_name: args.queue_name,
            routing_key: args.routing_key,
        };

        em::unbind_queue(&self.em, cmd).await?;

        self.send_frame(Frame::Frame(frame::queue_unbind_ok(channel))).await?;

        Ok(())
    }

    pub async fn queue_purge(&mut self, channel: Channel, args: frame::QueuePurgeArgs) -> Result<()> {
        use crate::queue::handler;

        // Purge the not-yet sent messages from the queue. Queue gives back the number of purged
        // messages, so this operation is sync.

        let cmd = qm::GetQueueSinkQuery {
            channel,
            queue_name: args.queue_name,
        };

        match qm::get_command_sink(&self.qm, cmd).await {
            Ok(queue_sink) => {
                handler::purge(self.id.clone(), channel, &queue_sink).await.unwrap();
            }
            err => {
                handle_error!(self, err).unwrap();
            }
        }

        Ok(())
    }
}
