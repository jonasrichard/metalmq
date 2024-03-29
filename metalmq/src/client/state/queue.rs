use crate::client::state::Connection;
use crate::client::{self, ChannelError};
use crate::exchange::manager::{self as em, BindQueueCommand, UnbindQueueCommand};
use crate::queue::manager as qm;
use crate::{handle_error, Result};
use log::warn;
use metalmq_codec::codec::Frame;
use metalmq_codec::frame::{self, Channel};
use uuid::Uuid;

use super::ExclusiveQueue;

impl Connection {
    pub async fn queue_declare(&mut self, channel: Channel, args: frame::QueueDeclareArgs) -> Result<()> {
        let mut queue_name = args.name.clone();
        if queue_name.is_empty() {
            queue_name = Uuid::new_v4().hyphenated().to_string();
        }

        let passive = args.flags.contains(frame::QueueDeclareFlags::PASSIVE);
        let exclusive = args.flags.contains(frame::QueueDeclareFlags::EXCLUSIVE);

        let cmd = qm::QueueDeclareCommand {
            conn_id: self.id.clone(),
            channel,
            queue: args.into(),
            passive,
        };
        let (message_count, consumer_count) = qm::declare_queue(&self.qm, cmd).await?;

        if exclusive {
            self.exclusive_queues.push(ExclusiveQueue {
                queue_name: queue_name.clone(),
            });
        }

        self.send_frame(Frame::Frame(
            frame::QueueDeclareOkArgs::default()
                .name(&queue_name)
                .message_count(message_count)
                .consumer_count(consumer_count)
                .frame(channel),
        ))
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

                // TODO now we can use let patter = x else {} so we can drop this macro
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

        self.send_frame(Frame::Frame(
            frame::QueueDeleteOkArgs::default()
                .message_count(message_count)
                .frame(channel),
        ))
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

        self.send_frame(Frame::Frame(frame::AMQPFrame::Method(
            channel,
            frame::QUEUE_UNBIND_OK,
            frame::MethodFrameArgs::QueueUnbindOk,
        )))
        .await?;

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
                let message_count = handler::purge(self.id.clone(), channel, &queue_sink).await?;

                self.send_frame(Frame::Frame(
                    frame::QueuePurgeOkArgs::default()
                        .message_count(message_count)
                        .frame(channel),
                ))
                .await?;
            }
            err => {
                handle_error!(self, err).unwrap();
            }
        }

        Ok(())
    }
}
