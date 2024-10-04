use std::collections::HashMap;

use log::{debug, error};
use metalmq_codec::{
    codec::Frame,
    frame::{self, unify_class_method, AMQPFrame, ConnectionStartArgs, MethodFrameArgs},
};
use tokio::sync::mpsc;

use crate::{
    client::{
        channel::types::{Channel, Command},
        connection::types::{Connection, ConnectionState, ExclusiveQueue},
    },
    error::{to_runtime_error, ConnectionError, ErrorScope, Result, RuntimeError},
};

impl Connection {
    /// Handles a frame coming from the client and return with `Ok(true)` if the connection should
    /// keep open. In case of `Ok(false)` the connection loop should close the connection gently,
    /// but all the necessary communication had already happened.
    pub async fn handle_client_frame(&mut self, f: AMQPFrame) -> Result<bool> {
        use AMQPFrame::*;

        // During the processing of method frames can happen only that client closes the
        // connection. In that case the `handle_method_frame` returns with `Ok(false)` namely that
        // 'should we keep the loop running' = 'no'.
        //
        // So we need to return that value to the caller in order that connection loop can stop
        // itself. In this use-case we don't need to close the resources because
        // `handle_connection_close` already did that.
        //
        // If we got an error from anywhere else, we need to check what we need to clean up. If it
        // is a channel error, we just need to close the channel. If it is a connection error, we
        // need to iterate over the channels and close the channels, and then the connection.
        //
        // Is it a question if we need to send messages out to the client (probably no) during that
        // internal cleanup.
        let result = match f {
            Header => self.send_frame(Frame::Frame(ConnectionStartArgs::new().frame())).await,
            Method(ch, cm, ma) => match self.handle_method_frame(ch, cm, ma).await {
                Ok(keep_running) => return Ok(keep_running),
                Err(e) => Err(e),
            },
            ContentHeader(header) => {
                self.send_command_to_channel(header.channel, Command::ContentHeader(header))
                    .await
            }
            ContentBody(body) => {
                self.send_command_to_channel(body.channel, Command::ContentBody(body))
                    .await
            }
            Heartbeat(0) => Ok(()),
            Heartbeat(_) => ConnectionError::FrameError.into_result(0, "Heartbeat must have channel 0"),
        };

        // How to implement normal stop of connection? How to get out of the loop?

        if let Err(e) = result {
            debug!("Error in router: {e:?}");

            let rte = dbg!(to_runtime_error(e));

            match rte {
                RuntimeError {
                    scope: ErrorScope::Connection,
                    ..
                } => {
                    self.status = ConnectionState::ClosingByServer;

                    // TODO this should be silent close which means that it doesn't send out any
                    // frames
                    //
                    // TODO also this is the situation when client sent something which is not
                    // correct, so we send out a connection close and with error code. Here we
                    // shouldn't close the connection and the resources but we need to put the
                    // connection to 'Closing' state, and when the client send back the connection
                    // close ok, we can close the tcp socket.
                    //
                    // It is better if we have more states like Connected, Authenticated,
                    // ClosingByClient, ClosingByServer, etc.
                    let r2 = self.close().await;

                    dbg!(r2);

                    let _ = self.send_frame(dbg!(rte.into())).await.map_err(|e| dbg!(e));
                    // if it fails we just return with an error so loop will close everything

                    return Ok(false);
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

        Ok(true)
    }

    async fn handle_method_frame(&mut self, channel: u16, class_method: u32, ma: MethodFrameArgs) -> Result<bool> {
        use MethodFrameArgs::*;

        let mut ch_tx = None;

        // If it is not connection class frame, we need to look up the channel.
        if class_method >> 16 != 0x000A {
            ch_tx = self.channel_receivers.get(&channel);

            // We cannot unpack the Option, since we handle connection frames which obviously don't
            // belong to any channel.
            if class_method != frame::CHANNEL_OPEN && ch_tx.is_none() {
                return ConnectionError::ChannelError.into_result(class_method, "Channel not exist");
            }
        }

        match ma {
            ConnectionStart(_) => unreachable!(),
            ConnectionStartOk(args) => {
                self.handle_connection_start_ok(args).await?;

                Ok(true)
            }
            ConnectionTune(_) => unreachable!(),
            ConnectionTuneOk(args) => {
                self.handle_connection_tune_ok(args).await?;

                Ok(true)
            }
            ConnectionOpen(args) => {
                self.handle_connection_open(args).await?;

                Ok(true)
            }
            ConnectionOpenOk => unreachable!(),
            ConnectionClose(args) => {
                self.handle_connection_close(args).await?;

                Ok(false)
            }
            ConnectionCloseOk => {
                self.handle_connection_close_ok().await?;

                Ok(false)
            }
            ChannelOpen => {
                if ch_tx.is_some() {
                    ConnectionError::ChannelError.into_result(class_method, "CHANNEL_ERROR - Channel already exist")
                } else {
                    self.handle_channel_open(channel).await?;
                    //self.start_channel(channel).await?;

                    Ok(true)
                }
            }
            ChannelClose(args) => {
                let cmd = Command::Close(args.code, unify_class_method(args.class_id, args.method_id), args.text);

                self.send_command_to_channel(channel, cmd).await?;
                self.handle_channel_close(channel).await?;

                Ok(true)
            }
            ChannelCloseOk => {
                self.handle_channel_close_ok(channel).await?;

                Ok(true)
            }
            QueueDeclare(args) => {
                let exclusive = args.flags.contains(frame::QueueDeclareFlags::EXCLUSIVE);
                let queue_name = args.name.clone();
                let cmd = Command::MethodFrame(channel, class_method, MethodFrameArgs::QueueDeclare(args));

                self.send_command_to_channel(channel, cmd).await?;

                if exclusive {
                    self.exclusive_queues.push(ExclusiveQueue { queue_name });
                }

                Ok(true)
            }
            _ => {
                let cmd = Command::MethodFrame(channel, class_method, ma);

                self.send_command_to_channel(channel, cmd).await?;

                Ok(true)
            }
        }
    }

    pub async fn start_channel(&mut self, channel_number: u16) -> Result<()> {
        let mut channel = Channel {
            source_connection: self.id.clone(),
            number: channel_number,
            consumed_queue: None,
            passively_consumed_queue: None,
            in_flight_content: None,
            confirm_mode: false,
            next_confirm_delivery_tag: None,
            frame_size: self.frame_max,
            outgoing: self.outgoing.clone(),
            exchanges: HashMap::new(),
            em: self.em.clone(),
            qm: self.qm.clone(),
        };

        let (tx, rx) = mpsc::channel(16);

        let jh = tokio::spawn(async move {
            match channel.handle_message(rx).await {
                ok @ Ok(_) => ok,
                e => {
                    error!("{:?}", e);

                    let rte = to_runtime_error(e.unwrap_err());

                    channel.send_frame(rte.into()).await?;

                    Ok(())
                }
            }
        });

        self.channel_receivers.insert(channel_number, tx);
        self.channel_handlers.insert(channel_number, jh);

        Ok(())
    }

    /// Send frame out to client asynchronously.
    pub async fn send_frame(&self, f: Frame) -> Result<()> {
        self.outgoing.send(f).await?;

        Ok(())
    }

    pub async fn send_command_to_channel(&self, channel: u16, cmd: Command) -> Result<()> {
        if let Some(ch_tx) = self.channel_receivers.get(&channel) {
            if let Err(e) = ch_tx.send(cmd).await {
                error!("Cannot send frame to channel handler {:?}", e);

                ConnectionError::InternalError.into_result(0, "Internal error")
            } else {
                Ok(())
            }
        } else {
            ConnectionError::ChannelError.into_result(0, "Channel not exist")
        }
    }
}
