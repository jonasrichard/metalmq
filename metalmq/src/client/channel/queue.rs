use metalmq_codec::frame;

use crate::Result;

use super::Channel;

impl Channel {
    pub async fn queue_declare(&mut self, channel: Channel, args: frame::QueueDeclareArgs) -> Result<()> {
        Ok(())
    }

    pub async fn queue_bind(&mut self, channel: Channel, args: frame::QueueBindArgs) -> Result<()> {
        Ok(())
    }

    pub async fn queue_delete(&mut self, channel: Channel, args: frame::QueueDeleteArgs) -> Result<()> {
        Ok(())
    }

    pub async fn queue_unbind(&mut self, channel: Channel, args: frame::QueueUnbindArgs) -> Result<()> {
        Ok(())
    }

    pub async fn queue_purge(&mut self, channel: Channel, args: frame::QueuePurgeArgs) -> Result<()> {
        Ok(())
    }
}
