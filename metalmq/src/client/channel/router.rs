use metalmq_codec::codec::Frame;
use metalmq_codec::frame::unify_class_method;
use tokio::sync::mpsc;

use crate::client::channel::types::{Channel, Command};
use crate::error::Result;

impl Channel {
    pub async fn handle_message(&mut self, mut rx: mpsc::Receiver<Command>) -> Result<()> {
        use Command::*;

        while let Some(m) = rx.recv().await {
            match m {
                MethodFrame(_ch, _cm, ma) => {
                    let result = match ma {
                        metalmq_codec::frame::MethodFrameArgs::ChannelClose(args) => {
                            self.handle_channel_close(
                                args.code,
                                unify_class_method(args.class_id, args.method_id),
                                args.text,
                            )
                            .await
                        }
                        metalmq_codec::frame::MethodFrameArgs::ChannelCloseOk => unreachable!(),
                        metalmq_codec::frame::MethodFrameArgs::ExchangeDeclare(args) => {
                            self.handle_exchange_declare(args).await
                        }
                        metalmq_codec::frame::MethodFrameArgs::ExchangeDeclareOk => unreachable!(),
                        metalmq_codec::frame::MethodFrameArgs::ExchangeDelete(args) => {
                            self.handle_exchange_delete(args).await
                        }
                        metalmq_codec::frame::MethodFrameArgs::ExchangeDeleteOk => unreachable!(),
                        metalmq_codec::frame::MethodFrameArgs::QueueDeclare(args) => {
                            self.handle_queue_declare(args).await
                        }
                        metalmq_codec::frame::MethodFrameArgs::QueueDeclareOk(_) => unreachable!(),
                        metalmq_codec::frame::MethodFrameArgs::QueueBind(args) => self.handle_queue_bind(args).await,
                        metalmq_codec::frame::MethodFrameArgs::QueueBindOk => unreachable!(),
                        metalmq_codec::frame::MethodFrameArgs::QueuePurge(args) => self.handle_queue_purge(args).await,
                        metalmq_codec::frame::MethodFrameArgs::QueuePurgeOk(_) => unreachable!(),
                        metalmq_codec::frame::MethodFrameArgs::QueueDelete(args) => {
                            self.handle_queue_delete(args).await
                        }
                        metalmq_codec::frame::MethodFrameArgs::QueueDeleteOk(_) => unreachable!(),
                        metalmq_codec::frame::MethodFrameArgs::QueueUnbind(args) => {
                            self.handle_queue_unbind(args).await
                        }
                        metalmq_codec::frame::MethodFrameArgs::QueueUnbindOk => unreachable!(),
                        metalmq_codec::frame::MethodFrameArgs::BasicConsume(args) => {
                            self.handle_basic_consume(args).await
                        }
                        metalmq_codec::frame::MethodFrameArgs::BasicConsumeOk(_) => unreachable!(),
                        metalmq_codec::frame::MethodFrameArgs::BasicCancel(args) => {
                            self.handle_basic_cancel(args).await
                        }
                        metalmq_codec::frame::MethodFrameArgs::BasicCancelOk(_) => unreachable!(),
                        metalmq_codec::frame::MethodFrameArgs::BasicGet(args) => self.handle_basic_get(args).await,
                        metalmq_codec::frame::MethodFrameArgs::BasicGetOk(_) => unreachable!(),
                        metalmq_codec::frame::MethodFrameArgs::BasicGetEmpty => todo!(),
                        metalmq_codec::frame::MethodFrameArgs::BasicPublish(args) => {
                            self.handle_basic_publish(args).await
                        }
                        metalmq_codec::frame::MethodFrameArgs::BasicReturn(_) => unreachable!(),
                        metalmq_codec::frame::MethodFrameArgs::BasicDeliver(_) => unreachable!(),
                        metalmq_codec::frame::MethodFrameArgs::BasicAck(args) => self.handle_basic_ack(args).await,
                        metalmq_codec::frame::MethodFrameArgs::BasicReject(args) => {
                            self.handle_basic_reject(args).await
                        }
                        metalmq_codec::frame::MethodFrameArgs::BasicNack(_) => unimplemented!(),
                        metalmq_codec::frame::MethodFrameArgs::ConfirmSelect(args) => {
                            self.handle_confirm_select(args).await
                        }
                        metalmq_codec::frame::MethodFrameArgs::ConfirmSelectOk => unreachable!(),
                        _ => unreachable!(),
                    };

                    result?;
                }
                Close(reason, cm, text) => self.handle_channel_close(reason, cm, text).await?,
                ContentHeader(header) => self.handle_content_header(header).await?,
                ContentBody(body) => self.handle_content_body(body).await?,
            }
        }

        Ok(())
    }

    /// Send frame out to client asynchronously.
    pub async fn send_frame(&self, f: Frame) -> Result<()> {
        self.outgoing.send(f).await?;

        Ok(())
    }
}
