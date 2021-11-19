use crate::client::state::{Connection, MaybeFrame};
use crate::client::{self, ConnectionError};
use crate::exchange::manager::{self, DeleteExchangeCommand};
use crate::logerr;
use crate::queue::manager::{self as qm, QueueCancelConsume};
use anyhow::Result;
use log::{error, info};
use metalmq_codec::codec::Frame;
use metalmq_codec::frame::{self, Channel};

impl Connection {
    pub async fn channel_open(&mut self, channel: Channel) -> MaybeFrame {
        if self.open_channels.iter().any(|&c| c == channel) {
            client::connection_error(
                frame::CHANNEL_OPEN,
                ConnectionError::ChannelError,
                "Channel already opened",
            )
        } else {
            self.open_channels.push(channel);
            Ok(Some(Frame::Frame(frame::channel_open_ok(channel))))
        }
    }

    pub async fn channel_close(&mut self, channel: Channel, _args: frame::ChannelCloseArgs) -> MaybeFrame {
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

        Ok(Some(Frame::Frame(frame::channel_close_ok(channel))))
    }

    pub async fn channel_close_ok(&mut self, channel: Channel) -> MaybeFrame {
        self.consumed_queues.retain(|cq| cq.channel != channel);

        Ok(None)
    }

    pub async fn cleanup(&mut self) -> Result<()> {
        info!("Cleanup connection {}", self.id);

        for cq in &self.consumed_queues {
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
