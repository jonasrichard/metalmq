use crate::client::channel::runtime_error_to_frame;
use crate::RuntimeError;
use crate::{ErrorScope, Result};
use log::trace;

use super::types::Channel;

impl Channel {
    pub async fn handle_connection_close(&mut self) -> Result<()> {
        // TODO should be here just to close all channels, not repeating the channel close logic
        // Most of the time we have all channels closed at this point, but what if the connection
        // has been cut and client didn't have a chance to close everything properly?
        //for (channel, cq) in self.consumed_queues.drain() {
        //    let cmd = qm::QueueCancelConsume {
        //        channel,
        //        queue_name: cq.queue_name.clone(),
        //        consumer_tag: cq.consumer_tag.clone(),
        //    };

        //    logerr!(qm::cancel_consume(&self.qm, cmd).await);
        //}

        //for qs in &self.exclusive_queues {
        //    qm::queue_deleted(
        //        &self.qm,
        //        qm::QueueDeletedEvent {
        //            queue: qs.queue_name.clone(),
        //        },
        //    )
        //    .await
        //    .unwrap();
        //}

        // TODO cleanup, like close all channels, delete temporal queues, etc
        Ok(())
    }

    pub async fn handle_channel_close(&mut self, channel: u16) -> Result<()> {
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
}
