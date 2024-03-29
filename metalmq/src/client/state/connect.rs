use crate::client::state::Connection;
use crate::client::{self, ConnectionError};
use crate::Result;
use log::{info, trace};
use metalmq_codec::codec::Frame;
use metalmq_codec::frame::{self, Channel};

impl Connection {
    // TODO here we should send back the frames in outgoing channel with a buffer - avoid deadlock
    pub async fn connection_start_ok(&self, _channel: Channel, args: frame::ConnectionStartOkArgs) -> Result<()> {
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

    pub async fn connection_tune_ok(&mut self, _channel: Channel, args: frame::ConnectionTuneOkArgs) -> Result<()> {
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

    pub async fn connection_open(&self, _channel: Channel, args: frame::ConnectionOpenArgs) -> Result<()> {
        // TODO in case of virtual host which exists but the client doesn't have permission to work
        // with we need to send back an access-refused connection error.
        if args.virtual_host != "/" {
            self.send_frame(client::connection_error_frame(
                frame::CONNECTION_OPEN,
                ConnectionError::InvalidPath,
                "Cannot connect to virtualhost",
            ))
            .await
        } else {
            self.send_frame(Frame::Frame(frame::connection_open_ok())).await
        }
    }

    pub async fn connection_close(&mut self, _args: frame::ConnectionCloseArgs) -> Result<()> {
        info!("Connection {} is being closed", self.id);

        // TODO cleanup
        //   - consume handler -> remove as consumer, auto delete queues are deleted when there are
        //     no consumers there
        //   - exchange handler -> deregister (auto-delete exchange)
        //   - queues -> delete the exclusive queues
        self.handle_connection_close().await.unwrap();

        self.send_frame(Frame::Frame(frame::connection_close_ok())).await
    }
}
