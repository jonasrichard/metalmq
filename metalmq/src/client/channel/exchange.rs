use metalmq_codec::frame;

use crate::Result;

use super::types::Channel;

impl Channel {
    pub async fn handle_exchange_declare(&mut self, args: frame::ExchangeDeclareArgs) -> Result<()> {
        Ok(())
    }

    pub async fn handle_exchange_delete(&mut self, args: frame::ExchangeDeleteArgs) -> Result<()> {
        Ok(())
    }
}
