use metalmq_codec::frame;

use crate::{
    error::Result,
    exchange::{
        self,
        manager::{DeclareExchangeCommand, DeleteExchangeCommand},
    },
};

use super::types::Channel;

impl Channel {
    pub async fn handle_exchange_declare(&mut self, args: frame::ExchangeDeclareArgs) -> Result<()> {
        let no_wait = args.flags.contains(frame::ExchangeDeclareFlags::NO_WAIT);
        let passive = args.flags.contains(frame::ExchangeDeclareFlags::PASSIVE);
        let exchange_name = args.exchange_name.clone();

        exchange::validate_exchange_type(self.number, &args.exchange_type)?;
        exchange::validate_exchange_name(self.number, &args.exchange_name)?;

        let cmd = DeclareExchangeCommand {
            channel: self.number,
            exchange: args.into(),
            passive,
            outgoing: self.outgoing.clone(),
        };

        let ex_tx = crate::exchange::manager::declare_exchange(&self.em, cmd).await?;

        self.exchanges.insert(exchange_name.clone(), ex_tx);

        if !no_wait {
            self.outgoing
                .send(frame::exchange_declare_ok(self.number).into())
                .await?
        }

        Ok(())
    }

    pub async fn handle_exchange_delete(&mut self, args: frame::ExchangeDeleteArgs) -> Result<()> {
        let exchange_name = args.exchange_name.clone();
        let cmd = DeleteExchangeCommand {
            channel: self.number,
            if_unused: args.flags.contains(frame::ExchangeDeleteFlags::IF_UNUSED),
            exchange_name: args.exchange_name,
        };

        // At first we remove the exchange from the local cache in order that the next call fails
        // we don't have inconsistent state.
        self.exchanges.remove(&exchange_name);

        exchange::manager::delete_exchange(&self.em, cmd).await?;

        self.send_frame(frame::exchange_delete_ok(self.number).into()).await
    }
}
