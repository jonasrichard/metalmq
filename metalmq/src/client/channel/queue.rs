use log::warn;
use metalmq_codec::codec::Frame;
use metalmq_codec::frame;
use uuid::Uuid;

use crate::client::channel::types::Channel;
use crate::error::{ChannelError, Result};
use crate::exchange::manager::{BindQueueCommand, UnbindQueueCommand};
use crate::{exchange, queue};

impl Channel {
    pub async fn handle_queue_declare(&mut self, args: frame::QueueDeclareArgs) -> Result<()> {
        let mut queue_name = args.name.clone();
        if queue_name.is_empty() {
            queue_name = Uuid::new_v4().hyphenated().to_string();
        }

        let passive = args.flags.contains(frame::QueueDeclareFlags::PASSIVE);

        let cmd = queue::manager::QueueDeclareCommand {
            conn_id: self.source_connection.clone(),
            channel: self.number,
            queue: args.into(),
            passive,
        };
        let (message_count, consumer_count) = queue::manager::declare_queue(&self.qm, cmd).await?;

        self.send_frame(Frame::Frame(
            frame::QueueDeclareOkArgs::default()
                .name(&queue_name)
                .message_count(message_count)
                .consumer_count(consumer_count)
                .frame(self.number),
        ))
        .await?;

        Ok(())
    }

    pub async fn handle_queue_bind(&mut self, args: frame::QueueBindArgs) -> Result<()> {
        let cmd = queue::manager::GetQueueSinkQuery {
            channel: self.number,
            queue_name: args.queue_name.clone(),
        };

        match queue::manager::get_command_sink(&self.qm, cmd).await {
            Ok(sink) => {
                let cmd = BindQueueCommand {
                    conn_id: self.source_connection.clone(),
                    channel: self.number,
                    exchange_name: args.exchange_name,
                    queue_name: args.queue_name,
                    routing_key: args.routing_key,
                    args: args.args,
                    queue_sink: sink,
                };

                // TODO now we can use let patter = x else {} so we can drop this macro
                exchange::manager::bind_queue(&self.em, cmd).await?;

                self.send_frame(Frame::Frame(frame::queue_bind_ok(self.number))).await
            }
            Err(e) => {
                warn!("{:?}", e);

                ChannelError::NotFound.into_result(self.number, frame::QUEUE_BIND, "Queue not found")
            }
        }
    }

    pub async fn handle_queue_delete(&mut self, args: frame::QueueDeleteArgs) -> Result<()> {
        let cmd = queue::manager::QueueDeleteCommand {
            conn_id: self.source_connection.clone(),
            channel: self.number,
            queue_name: args.queue_name,
            if_unused: args.flags.contains(frame::QueueDeleteFlags::IF_UNUSED),
            if_empty: args.flags.contains(frame::QueueDeleteFlags::IF_EMPTY),
        };

        let message_count = queue::manager::delete_queue(&self.qm, cmd).await?;

        self.send_frame(Frame::Frame(
            frame::QueueDeleteOkArgs::default()
                .message_count(message_count)
                .frame(self.number),
        ))
        .await?;

        Ok(())
    }

    pub async fn handle_queue_unbind(&mut self, args: frame::QueueUnbindArgs) -> Result<()> {
        let cmd = UnbindQueueCommand {
            conn_id: self.source_connection.clone(),
            channel: self.number,
            exchange_name: args.exchange_name,
            queue_name: args.queue_name,
            routing_key: args.routing_key,
        };

        exchange::manager::unbind_queue(&self.em, cmd).await?;

        self.send_frame(Frame::Frame(frame::AMQPFrame::Method(
            self.number,
            frame::QUEUE_UNBIND_OK,
            frame::MethodFrameArgs::QueueUnbindOk,
        )))
        .await?;

        Ok(())
    }

    pub async fn handle_queue_purge(&mut self, args: frame::QueuePurgeArgs) -> Result<()> {
        // Purge the not-yet sent messages from the queue. Queue gives back the number of purged
        // messages, so this operation is sync.

        let cmd = queue::manager::GetQueueSinkQuery {
            channel: self.number,
            queue_name: args.queue_name,
        };

        let queue_sink = queue::manager::get_command_sink(&self.qm, cmd).await?;
        let message_count = queue::handler::purge(self.source_connection.clone(), self.number, &queue_sink).await?;

        self.send_frame(Frame::Frame(
            frame::QueuePurgeOkArgs::default()
                .message_count(message_count)
                .frame(self.number),
        ))
        .await?;

        Ok(())
    }
}
