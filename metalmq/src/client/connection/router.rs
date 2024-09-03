use std::collections::HashMap;

use metalmq_codec::{
    codec::Frame,
    frame::{AMQPFrame, ConnectionStartArgs, MethodFrameArgs},
};
use tokio::sync::mpsc;

use crate::{
    client::{channel::types::Channel, connection::types::Connection},
    error::{to_runtime_error, ConnectionError, ErrorScope, Result, RuntimeError},
};

impl Connection {
    pub async fn handle_client_frame(&mut self, f: AMQPFrame) -> Result<()> {
        // TODO here we need to handle the error
        // if it is connection error, close the conn and before sending the connection close
        // message, send all the cleaning, like channel close, basic consume end, ets
        // if it is a channel error, close the channel only and send out the channel close message
        //
        // if it is other error, log it and close the connection
        use AMQPFrame::*;

        let result = match &f {
            Header => self.send_frame(Frame::Frame(ConnectionStartArgs::new().frame())).await,
            Method(_, _, _) => self.handle_method_frame(f).await,
            ContentHeader(ch) => self.send_command_to_channel(ch.channel, f).await,
            ContentBody(cb) => self.send_command_to_channel(cb.channel, f).await,
            Heartbeat(0) => Ok(()),
            Heartbeat(_) => ConnectionError::FrameError.to_result(0, "Heartbeat must have channel 0"),
        };

        // How to implement normal stop of connection? How to get out of the loop?

        if let Err(e) = result {
            let rte = to_runtime_error(e);

            match rte {
                RuntimeError {
                    scope: ErrorScope::Connection,
                    ..
                } => {
                    let _r2 = self.close().await;

                    self.send_frame(rte.into()).await;
                    // if it fails we just return with an error so loop will close everything
                }

                RuntimeError {
                    scope: ErrorScope::Channel,
                    channel,
                    ..
                } => {
                    let r2 = self.close_channel(channel).await;

                    if let Err(_e) = r2 {
                        // close connection and send out error frame
                    }
                }
            };
        }

        Ok(())
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
                    ConnectionError::ChannelError.to_result(*class_method, "CHANNEL_ERROR - Channel is already opened")
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

    async fn start_channel(&mut self, channel_number: u16) -> Result<()> {
        self.handle_channel_open(channel_number).await?;

        let mut channel = Channel {
            source_connection: self.id.clone(),
            number: channel_number,
            consumed_queue: None,
            in_flight_content: None,
            confirm_mode: false,
            next_confirm_delivery_tag: 1u64,
            outgoing: self.outgoing.clone(),
            exchanges: HashMap::new(),
            em: self.em.clone(),
            qm: self.qm.clone(),
        };

        let (tx, rx) = mpsc::channel(16);

        let jh = tokio::spawn(async move { channel.handle_message(rx).await });

        self.channel_receivers.insert(channel_number, tx);
        self.channel_handlers.insert(channel_number, jh);

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
            return ConnectionError::ChannelError.to_result(0, "No such channel");
        }

        Ok(())
    }
}
