use crate::client;
use crate::client::state::Connection;
use crate::exchange;
use crate::exchange::manager::{self, DeclareExchangeCommand, DeleteExchangeCommand};
use crate::{handle_error, logerr, Result};
use log::error;
use metalmq_codec::codec::Frame;
use metalmq_codec::frame::{self, Channel};

impl Connection {
    pub async fn exchange_delete(&mut self, channel: Channel, args: frame::ExchangeDeleteArgs) -> Result<()> {
        let exchange_name = args.exchange_name.clone();
        let cmd = DeleteExchangeCommand {
            channel,
            if_unused: args.flags.contains(frame::ExchangeDeleteFlags::IF_UNUSED),
            exchange_name: args.exchange_name,
        };

        logerr!(handle_error!(self, manager::delete_exchange(&self.em, cmd).await));

        // TODO what happens if the previous code returns with an error and we never removes that
        // exchange?
        self.exchanges.remove(&exchange_name);

        self.send_frame(Frame::Frame(frame::exchange_delete_ok(channel))).await
    }
}
