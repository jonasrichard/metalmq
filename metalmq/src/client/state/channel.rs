use crate::client;
use crate::client::state::Connection;
use crate::exchange::manager::{self, DeleteExchangeCommand};
use crate::logerr;
use crate::queue::manager::{self as qm, QueueCancelConsume};
use crate::Result;
use log::{debug, error, info};
use metalmq_codec::codec::Frame;
use metalmq_codec::frame::{self, Channel};

impl Connection {
    pub async fn channel_open(&mut self, channel: Channel) -> Result<()> {
        if self.open_channels.iter().any(|&c| c == channel) {
            let (class_id, method_id) = frame::split_class_method(frame::CHANNEL_OPEN);
            let err = crate::RuntimeError {
                scope: crate::ErrorScope::Connection,
                channel: 0,
                code: client::ConnectionError::ChannelError as u16,
                text: "CHANNEL_ERROR - Channel is already opened".to_owned(),
                class_id,
                method_id,
            };

            self.send_frame(client::runtime_error_to_frame(&err)).await?;

            return Err(Box::new(err));
        } else {
            self.open_channels.push(channel);
            self.send_frame(Frame::Frame(Box::new(frame::channel_open_ok(channel))))
                .await?;
        }

        Ok(())

        // TODO here we need to send out the frame, at least to the output channel,
        // so we can be sure that the order of the emitted frames are correct.
        // Also it can give us sync points, like I need to send out basic-consume-ok,
        // so then I can tell the queue that now you can send out basic-deliver
        // frames.
    }

    pub async fn channel_close(&mut self, channel: Channel, _args: frame::ChannelCloseArgs) -> Result<()> {
        // TODO close all exchanges and queues it needs to.
        if !self.auto_delete_exchanges.is_empty() {
            for exchange_name in &self.auto_delete_exchanges {
                // TODO this is bad here, we hold the locks until the exchanges are not deleted
                // I don't know if await yield release that locks but I doubt it.
                let cmd = DeleteExchangeCommand {
                    channel,
                    if_unused: false,
                    exchange_name: exchange_name.to_owned(),
                };

                logerr!(manager::delete_exchange(&self.em, cmd).await);
            }
        }

        for cq in &self.consumed_queues {
            if cq.channel == channel {
                let cmd = QueueCancelConsume {
                    channel,
                    queue_name: cq.queue_name.clone(),
                    consumer_tag: cq.consumer_tag.clone(),
                };

                logerr!(qm::cancel_consume(&self.qm, cmd).await);
            }
        }

        self.consumed_queues.retain(|cq| cq.channel != channel);

        // TODO delete exclusive queues

        self.open_channels.retain(|c| c != &channel);

        self.send_frame(Frame::Frame(Box::new(frame::channel_close_ok(channel))))
            .await?;

        Ok(())
    }

    pub async fn channel_close_ok(&mut self, channel: Channel) -> Result<()> {
        self.consumed_queues.retain(|cq| cq.channel != channel);

        Ok(())
    }

    pub async fn cleanup(&mut self) -> Result<()> {
        info!("Cleanup connection {}", self.id);

        for cq in &self.consumed_queues {
            debug!(
                "Cancel consumer channel: {} queue: {} consumer tag: {}",
                cq.channel, cq.queue_name, cq.consumer_tag
            );

            let cmd = QueueCancelConsume {
                channel: cq.channel,
                queue_name: cq.queue_name.clone(),
                consumer_tag: cq.consumer_tag.clone(),
            };

            logerr!(qm::cancel_consume(&self.qm, cmd).await);
        }

        Ok(())
    }
}
