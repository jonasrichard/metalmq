use crate::client::state::{ChannelState, Connection};
use crate::client::{self, ConnectionError};
use crate::logerr;
use crate::queue::manager::{self as qm, QueueCancelConsume};
use crate::{handle_error, Result};
use log::{debug, error, info};
use metalmq_codec::codec::Frame;
use metalmq_codec::frame::{self, Channel};

impl Connection {
    pub async fn channel_open(&mut self, channel: Channel) -> Result<()> {
        if self.open_channels.contains_key(&channel) {
            let err = client::connection_error(
                frame::CHANNEL_OPEN,
                ConnectionError::ChannelError,
                "CHANNEL_ERROR - Channel is already opened",
            );

            return handle_error!(self, err);
        } else {
            self.open_channels.insert(
                channel,
                ChannelState {
                    channel,
                    confirm_mode: false,
                    frame_sink: self.outgoing.clone(),
                },
            );

            self.send_frame(Frame::Frame(frame::channel_open_ok(channel))).await?;
        }

        Ok(())
    }

    pub async fn channel_close(&mut self, channel: Channel, _args: frame::ChannelCloseArgs) -> Result<()> {
        self.handle_channel_close(channel).await?;

        //// TODO delete exclusive queues

        self.open_channels.remove(&channel);

        self.send_frame(Frame::Frame(frame::channel_close_ok(channel))).await?;

        Ok(())
    }

    pub async fn channel_close_ok(&mut self, channel: Channel) -> Result<()> {
        // TODO not sure if we need to send out basic cancel here
        self.open_channels.remove(&channel);

        Ok(())
    }

    pub async fn cleanup(&mut self) -> Result<()> {
        info!("Cleanup connection {}", self.id);

        for (channel, cq) in &self.consumed_queues {
            debug!(
                "Cancel consumer channel: {} queue: {} consumer tag: {}",
                channel, cq.queue_name, cq.consumer_tag
            );

            let cmd = QueueCancelConsume {
                channel: *channel,
                queue_name: cq.queue_name.clone(),
                consumer_tag: cq.consumer_tag.clone(),
            };

            logerr!(qm::cancel_consume(&self.qm, cmd).await);
        }

        Ok(())
    }
}
