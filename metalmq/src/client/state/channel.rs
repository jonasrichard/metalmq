use crate::client::state::{Connection, MaybeFrame};
use crate::client::{self, ConnectionError};
use crate::exchange::manager;
use crate::logerr;
use log::error;
use metalmq_codec::codec::Frame;
use metalmq_codec::frame::{self, Channel};

impl Connection {
    pub(crate) async fn channel_open(&mut self, channel: Channel) -> MaybeFrame {
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

    pub(crate) async fn channel_close(&mut self, channel: Channel, _args: frame::ChannelCloseArgs) -> MaybeFrame {
        // TODO close all exchanges and queues it needs to.
        if !self.auto_delete_exchanges.is_empty() {
            for exchange_name in &self.auto_delete_exchanges {
                // TODO this is bad here, we hold the locks until the exchanges are not deleted
                // I don't know if await yield release that locks but I doubt it.
                logerr!(manager::delete_exchange(&self.em, channel, exchange_name).await);
            }
        }

        self.consumed_queues.retain(|cq| cq.channel != channel);

        self.open_channels.retain(|c| c != &channel);

        // TODO use debug_assert! for checking state

        Ok(Some(Frame::Frame(frame::channel_close_ok(channel))))
    }

    pub(crate) async fn channel_close_ok(&mut self, channel: Channel) -> MaybeFrame {
        self.consumed_queues.retain(|cq| cq.channel != channel);

        Ok(None)
    }
}
