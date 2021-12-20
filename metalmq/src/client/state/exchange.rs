use crate::client;
use crate::client::state::Connection;
use crate::exchange::manager::{self, DeclareExchangeCommand, DeleteExchangeCommand};
use crate::Result;
use metalmq_codec::codec::Frame;
use metalmq_codec::frame::{self, Channel};

impl Connection {
    pub async fn exchange_declare(&mut self, channel: Channel, args: frame::ExchangeDeclareArgs) -> Result<()> {
        let no_wait = args.flags.contains(frame::ExchangeDeclareFlags::NO_WAIT);
        let passive = args.flags.contains(frame::ExchangeDeclareFlags::PASSIVE);
        let exchange_name = args.exchange_name.clone();

        let cmd = DeclareExchangeCommand {
            channel,
            exchange: args.into(),
            passive,
            outgoing: self.outgoing.clone(),
        };
        let result = manager::declare_exchange(&self.em, cmd).await;

        match result {
            Ok(ch) => {
                self.exchanges.insert(exchange_name.clone(), ch);

                if no_wait {
                    Ok(())
                } else {
                    self.send_frame(Frame::Frame(frame::exchange_declare_ok(channel))).await
                }
            }
            Err(err) => {
                let rte = client::to_runtime_error(err);

                self.handle_error(rte).await
            }
        }
    }

    pub async fn exchange_delete(&mut self, channel: Channel, args: frame::ExchangeDeleteArgs) -> Result<()> {
        // FIXME this is a hack for borrow checker. We need to pass the reference of the command
        // to the enum. The command is read-only, so it must be easy.
        let exchange_name = args.exchange_name.clone();
        let cmd = DeleteExchangeCommand {
            channel,
            if_unused: args.flags.contains(frame::ExchangeDeleteFlags::IF_UNUSED),
            exchange_name: args.exchange_name,
        };

        manager::delete_exchange(&self.em, cmd).await?;

        self.exchanges.remove(&exchange_name);

        self.send_frame(Frame::Frame(frame::exchange_delete_ok(channel))).await
    }
}
