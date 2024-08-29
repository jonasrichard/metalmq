use metalmq_codec::{
    codec::Frame,
    frame::{AMQPFrame, ConnectionStartArgs, MethodFrameArgs},
};

use crate::{client::channel::types::Channel, Result};

use super::{connection_error, types::Connection, ConnectionError};

impl Connection {
    pub async fn handle_client_frame(&mut self, f: AMQPFrame) -> Result<()> {
        use AMQPFrame::*;

        match &f {
            Header => self.send_frame(Frame::Frame(ConnectionStartArgs::new().frame())).await,
            Method(_, _, _) => self.handle_method_frame(f).await,
            ContentHeader(ch) => self.send_command_to_channel(ch.channel, f).await,
            ContentBody(cb) => self.send_command_to_channel(cb.channel, f).await,
            Heartbeat(0) => Ok(()),
            Heartbeat(_) => connection_error(0, ConnectionError::FrameError, "Heartbeat must have channel 0"),
        }
    }

    async fn handle_method_frame(&mut self, f: AMQPFrame) -> Result<()> {
        let (channel, class_method) = match &f {
            AMQPFrame::Method(channel, cm, _) => (channel, cm),
            _ => unreachable!(),
        };

        // WARN this is quite ugly and the intent is not clear
        match class_method >> 16 {
            0x000A => self.handle_connection_command(f).await,
            _ if *class_method == metalmq_codec::frame::CHANNEL_OPEN => {
                if self.channel_receivers.contains_key(channel) {
                    connection_error(
                        *class_method,
                        ConnectionError::ChannelError,
                        "CHANNEL_ERROR - Channel is already opened",
                    )
                } else {
                    self.start_channel(*channel).await
                }
            }
            _ => self.send_command_to_channel(*channel, f).await,
        }
    }

    async fn handle_connection_command(&mut self, f: AMQPFrame) -> Result<()> {
        use MethodFrameArgs::*;

        match f {
            AMQPFrame::Method(_, _cm, mf) => match mf {
                ConnectionStartOk(args) => self.handle_connection_start_ok(args).await,
                ConnectionTuneOk(args) => self.handle_connection_tune_ok(args).await,
                ConnectionOpen(args) => self.handle_connection_open(args).await,
                ConnectionClose(args) => self.handle_connection_close(args).await,
                _ => unreachable!(),
            },
            _ => {
                unreachable!()
            }
        }
    }

    async fn start_channel(&mut self, channel: u16) -> Result<()> {
        self.handle_channel_open(channel).await?;

        let (ch_tx, jh) = Channel::start(channel, self.outgoing.clone()).await;

        self.channel_receivers.insert(channel, ch_tx);
        self.channel_handlers.insert(channel, jh);

        Ok(())
    }

    /// Send frame out to client asynchronously.
    pub async fn send_frame(&self, f: Frame) -> Result<()> {
        self.outgoing.send(f).await?;

        Ok(())
    }

    async fn send_command_to_channel(&self, ch: u16, f: AMQPFrame) -> Result<()> {
        if let Some(ch_tx) = self.channel_receivers.get(&ch) {
            ch_tx.send(f).await?;
        } else {
            return connection_error(0, ConnectionError::ChannelError, "No such channel");
        }

        Ok(())
    }
}
