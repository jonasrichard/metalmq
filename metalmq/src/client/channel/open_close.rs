use super::types::Channel;
use crate::error::Result;

impl Channel {
    pub async fn handle_channel_close(&mut self, _code: u16, _cm: u32, _text: String) -> Result<()> {
        // Cancel consumed queues on the channel
        //if let Some(cq) = self.consumed_queues.remove(&channel) {
        //    qm::cancel_consume(
        //        &self.qm,
        //        qm::QueueCancelConsume {
        //            channel,
        //            queue_name: cq.queue_name.clone(),
        //            consumer_tag: cq.consumer_tag.clone(),
        //        },
        //    )
        //    .await?;
        //}

        //// Cancel passive consumers registered because of a Basic.Get
        //if let Some(pq) = self.passively_consumed_queues.remove(&channel) {
        //    pq.queue_sink
        //        .send(queue_handler::QueueCommand::PassiveCancelConsume(
        //            queue_handler::PassiveCancelConsumeCmd {
        //                conn_id: self.id.clone(),
        //                channel,
        //            },
        //        ))
        //        .await?;
        //}

        Ok(())
    }

    pub async fn handle_channel_close_ok(&mut self, _channel: u16) -> Result<()> {
        todo!();

        //Ok(())
    }
}
