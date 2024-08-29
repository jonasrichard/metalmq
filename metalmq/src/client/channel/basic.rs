use metalmq_codec::frame;

use crate::Result;

use super::types::Channel;

impl Channel {
    pub async fn handle_basic_publish(&mut self, args: frame::BasicPublishArgs) -> Result<()> {
        Ok(())
    }
    pub async fn handle_basic_consume(&mut self, args: frame::BasicConsumeArgs) -> Result<()> {
        Ok(())
    }
    pub async fn handle_basic_cancel(&mut self, args: frame::BasicCancelArgs) -> Result<()> {
        Ok(())
    }
    /// Handles Ack coming from client.
    ///
    /// A message can be acked more than once. If a non-delivered message is acked, a channel
    /// exception will be raised.
    pub async fn handle_basic_ack(&mut self, args: frame::BasicAckArgs) -> Result<()> {
        Ok(())
    }
    pub async fn handle_basic_get(&mut self, args: frame::BasicGetArgs) -> Result<()> {
        Ok(())
    }
    pub async fn handle_basic_reject(&mut self, __args: frame::BasicRejectArgs) -> Result<()> {
        Ok(())
    }
    pub async fn handle_confirm_select(&mut self, _args: frame::ConfirmSelectArgs) -> Result<()> {
        Ok(())
    }
    pub async fn handle_content_header(&mut self, header: frame::ContentHeaderFrame) -> Result<()> {
        Ok(())
    }
    pub async fn handle_content_body(&mut self, body: frame::ContentBodyFrame) -> Result<()> {
        Ok(())
    }
}
