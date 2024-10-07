use log::{error, info, trace, warn};
use metalmq_codec::{
    codec::Frame,
    frame::{self, ConnectionCloseArgs, ConnectionOpenArgs, ConnectionStartOkArgs, ConnectionTuneOkArgs},
};

use crate::{
    client::connection::types::{Connection, ConnectionState},
    error::{ConnectionError, Result},
};

impl Connection {
    pub async fn handle_connection_start_ok(&mut self, args: ConnectionStartOkArgs) -> Result<()> {
        self.status = ConnectionState::Connected;

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
                self.status = ConnectionState::Authenticated;
            }
        }

        match self.status {
            ConnectionState::Authenticated => self.send_frame(Frame::Frame(frame::connection_tune())).await,
            _ => ConnectionError::AccessRefused.into_result(
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
            ConnectionError::InvalidPath.into_result(frame::CONNECTION_OPEN, "Cannot connect to virtualhost")
        } else {
            self.send_frame(Frame::Frame(frame::connection_open_ok())).await
        }
    }

    pub async fn handle_connection_close(&mut self, args: ConnectionCloseArgs) -> Result<()> {
        self.status = ConnectionState::ClosingByClient;

        info!("Connection {} is being closed", self.id);

        if let Err(e) = self.forced_close().await {
            error!("Error forcedly close connection resources: {e:?}");
        }

        self.status = ConnectionState::Closed;

        self.send_frame(Frame::Frame(frame::connection_close_ok())).await
    }

    pub async fn handle_connection_close_ok(&mut self) -> Result<()> {
        self.status = ConnectionState::Closed;

        if let Err(e) = self.forced_close().await {
            error!("Error forcedly close connection resources: {e:?}");
        }

        Ok(())
    }

    pub async fn handle_channel_open(&mut self, channel: u16) -> Result<()> {
        // Client cannot open a channel whose number is higher than the maximum allowed.
        if channel > self.channel_max {
            warn!("Channel number is too big: {channel}");

            return ConnectionError::ResourceError
                .into_result(frame::CHANNEL_OPEN, "RESOURCE_ERROR - Channel number is too large");
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
        if let Err(e) = self.close_channel(channel).await {
            error!("Error during closing channel {e:?}");
        }

        Ok(())
    }
}
