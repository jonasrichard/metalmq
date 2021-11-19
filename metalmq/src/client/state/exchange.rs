use crate::client::state::{Connection, MaybeFrame};
use crate::exchange::manager::{self, DeclareExchangeCommand, DeleteExchangeCommand};
use crate::{ErrorScope, RuntimeError};
use metalmq_codec::codec::Frame;
use metalmq_codec::frame::{self, AMQPFrame, Channel};

impl Connection {
    pub async fn exchange_declare(&mut self, channel: Channel, args: frame::ExchangeDeclareArgs) -> MaybeFrame {
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
                    Ok(None)
                } else {
                    Ok(Some(Frame::Frame(frame::exchange_declare_ok(channel))))
                }
            }
            // TODO is it automatic now?
            Err(e) => match e.downcast::<RuntimeError>() {
                Ok(mut rte) => match rte.scope {
                    ErrorScope::Connection => Ok(Some(Frame::Frame(AMQPFrame::from(*rte)))),
                    ErrorScope::Channel => {
                        rte.channel = channel;
                        Ok(Some(Frame::Frame(AMQPFrame::from(*rte))))
                    }
                },
                Err(e2) => Err(e2),
            },
        }
    }

    pub async fn exchange_delete(&mut self, channel: Channel, args: frame::ExchangeDeleteArgs) -> MaybeFrame {
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

        Ok(Some(Frame::Frame(frame::exchange_delete_ok(channel))))
    }
}
