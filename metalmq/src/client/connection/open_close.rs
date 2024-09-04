use log::{info, trace, warn};
use metalmq_codec::{
    codec::Frame,
    frame::{self, ConnectionCloseArgs, ConnectionOpenArgs, ConnectionStartOkArgs, ConnectionTuneOkArgs},
};

use crate::{
    client::connection::types::Connection,
    error::{ConnectionError, Result},
};

impl Connection {
    pub async fn handle_connection_start_ok(&mut self, args: ConnectionStartOkArgs) -> Result<()> {
        let mut authenticated = false;

        if args.mechanism.eq(&"PLAIN") {
            let mut it = args.response.as_bytes().split(|b| b == &0u8);
            it.next();
            let username = it.next();
            let password = it.next();

            trace!(
                "User {:?} Pass {:?}",
                username.map(String::from_utf8_lossy),
                password.map(String::from_utf8_lossy)
            );

            // TODO get users from config file
            if let (Some(b"guest"), Some(b"guest")) = (username, password) {
                authenticated = true;
            }
        }

        match authenticated {
            true => self.send_frame(Frame::Frame(frame::connection_tune())).await,
            false => ConnectionError::AccessRefused.to_result(
                frame::CONNECTION_START_OK,
                "ACCESS_REFUSED - Username and password are incorrect",
            ),
        }
    }

    pub async fn handle_connection_tune_ok(&mut self, args: ConnectionTuneOkArgs) -> Result<()> {
        if args.heartbeat == 0 {
            self.heartbeat_interval = None;
        } else {
            self.heartbeat_interval = Some(std::time::Duration::from_secs(args.heartbeat as u64));
        }

        // If client wants lower channel-max, we need to accept that.
        if args.channel_max != 0 && args.channel_max < self.channel_max {
            self.channel_max = args.channel_max;
        }

        // Server recommends a maximum frame size, and client can lower that value.
        if (args.frame_max as usize) < self.frame_max {
            self.frame_max = args.frame_max as usize;
        }

        Ok(())
    }

    pub async fn handle_connection_open(&mut self, args: ConnectionOpenArgs) -> Result<()> {
        // TODO in case of virtual host which exists but the client doesn't have permission to work
        // with we need to send back an access-refused connection error.
        if args.virtual_host != "/" {
            ConnectionError::InvalidPath.to_result(frame::CONNECTION_OPEN, "Cannot connect to virtualhost")
        } else {
            self.send_frame(Frame::Frame(frame::connection_open_ok())).await
        }
    }

    pub async fn handle_connection_close(&mut self, args: ConnectionCloseArgs) -> Result<()> {
        info!("Connection {} is being closed", self.id);

        // TODO cleanup
        //   - consume handler -> remove as consumer, auto delete queues are deleted when there are
        //     no consumers there
        //   - exchange handler -> deregister (auto-delete exchange)
        //   - queues -> delete the exclusive queues

        self.send_frame(Frame::Frame(frame::connection_close_ok())).await
    }

    pub async fn handle_channel_open(&mut self, channel: u16) -> Result<()> {
        // Client cannot open a channel whose number is higher than the maximum allowed.
        if channel > self.channel_max {
            warn!("Channel number is too big: {channel}");

            return ConnectionError::NotAllowed
                .to_result(frame::CHANNEL_OPEN, "NOT_ALLOWED - Channel number is too large");
        }

        self.start_channel(channel).await?;

        self.send_frame(Frame::Frame(frame::channel_open_ok(channel))).await?;

        Ok(())
    }

    pub async fn handle_channel_close(&mut self, channel: u16) -> Result<()> {
        // TODO delete exclusive queues

        self.close_channel(channel).await?;
        self.send_frame(Frame::Frame(frame::channel_close_ok(channel))).await?;

        Ok(())
    }

    pub async fn handle_channel_close_ok(&mut self, channel: u16) -> Result<()> {
        // TODO not sure if we need to send out basic cancel here
        self.channel_handlers.remove(&channel);
        self.channel_receivers.remove(&channel);

        Ok(())
    }

    pub async fn close(&mut self) -> Result<()> {
        info!("Cleanup connection {}", self.id);

        for (channel, ch_tx) in &self.channel_receivers {
            // drop channel channel in order to stop it
            let _ = ch_tx;

            if let Some(jh) = self.channel_handlers.remove(&channel) {
                jh.await;
            }
        }

        //for (channel, cq) in &self.consumed_queues {
        //    debug!(
        //        "Cancel consumer channel: {} queue: {} consumer tag: {}",
        //        channel, cq.queue_name, cq.consumer_tag
        //    );

        //    let cmd = QueueCancelConsume {
        //        channel: *channel,
        //        queue_name: cq.queue_name.clone(),
        //        consumer_tag: cq.consumer_tag.clone(),
        //    };

        //    logerr!(qm::cancel_consume(&self.qm, cmd).await);
        //}

        Ok(())
    }

    pub async fn close_channel(&mut self, channel: u16) -> Result<()> {
        if let Some(ch_tx) = self.channel_receivers.remove(&channel) {
            drop(ch_tx);
        }

        if let Some(jh) = self.channel_handlers.remove(&channel) {
            jh.await;
        }

        Ok(())
    }

    // Handle a runtime error a connection or a channel error. At first it sends the error frame
    // and then handle the closing of a channel or connection depending what kind of exception
    // happened.
    //
    // This function just sends out the error frame and return with `Err` if it is a connection
    // error, or it returns with `Ok` if it is a channel error. This is handy if we want to handle
    // the output with a `?` operator and we want to die in case of a connection error (aka we
    // want to propagate the error to the client handler).
    //async fn handle_error(&mut self, err: RuntimeError) -> Result<()> {
    //    trace!("Handling error {:?}", err);

    //    self.send_frame(runtime_error_to_frame(&err)).await?;

    //    match err.scope {
    //        ErrorScope::Connection => {
    //            self.close().await?;

    //            Err(Box::new(err))
    //        }
    //        ErrorScope::Channel => {
    //            self.close_channel(err.channel).await?;

    //            Ok(())
    //        }
    //    }
    //}
}
