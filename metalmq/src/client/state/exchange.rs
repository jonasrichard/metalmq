use crate::client;
use crate::client::state::Connection;
use crate::exchange;
use crate::exchange::manager::{self, DeclareExchangeCommand, DeleteExchangeCommand};
use crate::{handle_error, logerr, Result};
use log::error;
use metalmq_codec::codec::Frame;
use metalmq_codec::frame::{self, Channel};

impl Connection {
    pub async fn exchange_declare(&mut self, channel: Channel, args: frame::ExchangeDeclareArgs) -> Result<()> {
        use crate::exchange::manager::DeclareExchangeCommand;

        let no_wait = args.flags.contains(frame::ExchangeDeclareFlags::NO_WAIT);
        let passive = args.flags.contains(frame::ExchangeDeclareFlags::PASSIVE);
        let exchange_name = args.exchange_name.clone();

        logerr!(handle_error!(
            self,
            exchange::validate_exchange_type(&args.exchange_type)
        ));

        logerr!(handle_error!(
            self,
            exchange::validate_exchange_name(self.number, &args.exchange_name)
        ));

        let cmd = DeclareExchangeCommand {
            channel,
            exchange: args.into(),
            passive,
            outgoing: self.outgoing.clone(),
        };
        let result = crate::exchange::manager::declare_exchange(&self.em, cmd).await;

        match result {
            Ok(ch) => {
                self.exchanges.insert(exchange_name.clone(), ch);

                if no_wait {
                    Ok(())
                } else {
                    self.outgoing
                        .send(Frame::Frame(frame::exchange_declare_ok(channel)))
                        .await?
                }
            }
            Err(err) => {
                let rte = client::to_runtime_error(err);

                self.handle_error(rte).await
            }
        }
    }

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
