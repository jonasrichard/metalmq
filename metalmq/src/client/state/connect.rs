use crate::client::state::Connection;
use crate::client::{self, ConnectionError};
use crate::queue::manager;
use crate::Result;
use log::{error, info, trace};
use metalmq_codec::codec::Frame;
use metalmq_codec::frame::{self, Channel};

impl Connection {
    // TODO here we should send back the frames in outgoing channel with a buffer - avoid deadlock
    pub async fn connection_start_ok(&self, channel: Channel, args: frame::ConnectionStartOkArgs) -> Result<()> {
        let mut authenticated = false;

        if args.mechanism.eq(&"PLAIN") {
            let mut it = args.response.as_bytes().split(|b| b == &0u8);
            it.next();
            let username = it.next();
            let password = it.next();

            trace!("User {:?} Pass {:?}", username, password);

            if let (Some(b"guest"), Some(b"guest")) = (username, password) {
                authenticated = true;
            }
        }

        match authenticated {
            true => self.send_frame(Frame::Frame(frame::connection_tune(channel))).await,
            false => {
                self.send_frame(client::connection_error_frame(
                    0u32,
                    ConnectionError::AccessRefused,
                    "ACCESS_REFUSED - Username and password are incorrect",
                ))
                .await
            }
        }
    }

    pub async fn connection_open(&self, channel: Channel, args: frame::ConnectionOpenArgs) -> Result<()> {
        if args.virtual_host != "/" {
            self.send_frame(client::connection_error_frame(
                frame::CONNECTION_OPEN,
                ConnectionError::NotAllowed,
                "Cannot connect to virtualhost",
            ))
            .await
        } else {
            self.send_frame(Frame::Frame(frame::connection_open_ok(channel))).await
        }
    }

    pub async fn connection_close(&self, _args: frame::ConnectionCloseArgs) -> Result<()> {
        info!("Connection {} is being closed", self.id);

        // TODO cleanup
        //   - consume handler -> remove as consumer, auto delete queues are deleted when there are
        //     no consumers there
        //   - exchange handler -> deregister (auto-delete exchange)
        //   - queues -> delete the exclusive queues
        for cq in &self.consumed_queues {
            trace!("Cleaning up consumers {:?}", cq);

            let cmd = manager::QueueCancelConsume {
                channel: cq.channel,
                queue_name: cq.queue_name.clone(),
                consumer_tag: cq.consumer_tag.clone(),
            };
            if let Err(e) = manager::cancel_consume(&self.qm, cmd).await {
                error!("Err {:?}", e);
            }
        }

        self.send_frame(Frame::Frame(frame::connection_close_ok(0))).await
    }
}
