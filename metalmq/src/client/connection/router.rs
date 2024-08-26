use metalmq_codec::{
    codec::Frame,
    frame::{AMQPFrame, ConnectionStartArgs},
};

use crate::Result;

use super::{connection_error, types::Connection, ConnectionError};

impl Connection {
    async fn handle_client_frame(&mut self, f: AMQPFrame) -> Result<()> {
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
            0x000A => {
                self.handle_connection_command(f).await?;
            }
            _ if *class_method == metalmq_codec::frame::CHANNEL_OPEN => {}
            _ => {
                self.send_command_to_channel(*channel, f).await?;
            }
        }

        Ok(())
    }

    async fn handle_connection_command(&mut self, f: AMQPFrame) -> Result<()> {
        Ok(())
    }

    /// Send frame out to client asynchronously.
    async fn send_frame(&self, f: Frame) -> Result<()> {
        self.outgoing.send(f).await?;

        Ok(())
    }

    async fn send_command_to_channel(&self, ch: u16, f: AMQPFrame) -> Result<()> {
        if let Some(ch_tx) = self.channel_receivers.get(&ch) {
            ch_tx.send(f);
        } else {
            return connection_error(0, ConnectionError::ChannelError, "No such channel");
        }

        Ok(())
    }
}
