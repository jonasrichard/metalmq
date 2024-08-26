use std::collections::HashMap;

use metalmq_codec::frame;
use tokio::{sync::mpsc, task::JoinHandle};

use crate::Result;

struct Channel {
    number: u16,
}

pub async fn start() -> (JoinHandle<Result<()>>, mpsc::Sender<MethodArgs>) {
    let connection = Connection {
        channel_handlers: HashMap::new(),
        channel_receivers: HashMap::new(),
    };

    let (tx, rx) = mpsc::channel(16);

    let jh: JoinHandle<Result<()>> = tokio::spawn(async move {
        handle_message(connection, rx).await;

        Ok(())
    });

    (jh, tx)
}

async fn handle_message(mut connection: Connection, mut rx: mpsc::Receiver<MethodArgs>) -> Result<()> {
    loop {
        tokio::select! {
            maybe_method_frame = rx.recv() => {
                match maybe_method_frame {
                    Some((ch, cm, ma)) => {
                        // If class-method is 0x000A it is a connection frame
                        if cm >> 16 == 0x000A {
                            handle_connection_message(&mut connection, ma).await;
                        } else {
                            if let Some(tx) = connection.channel_receivers.get(&ch) {
                                tx.send((ch, cm, ma)).await;
                            } else {
                                let (ch_tx, ch_rx) = mpsc::channel(16);

                                let channel = Channel {
                                    number: ch,
                                };

                                let jh = tokio::spawn(async move {
                                    handle_channel_message(channel, ch_rx).await;
                                });

                                ch_tx.send((ch, cm, ma));

                                connection.channel_receivers.insert(ch, ch_tx);
                                connection.channel_handlers.insert(ch, jh);
                            }
                        }
                    }
                    None => {}
                }
            }
            // TODO the unified channel feedback channel
        }
    }

    // we need to make an mpsc channel which has message when a channel is about to close
    // then it can wait for the right join handle
    //while let Some(_m) = rx.recv().await {
    //    if 2 > 1 {
    //        let (tx, rx) = mpsc::channel(16);

    //        let jh = tokio::spawn(async move {
    //            // TODO start the channel here
    //        });

    //        connection.channel_handlers.insert(1, jh);
    //        connection.channel_receivers.insert(1, tx);
    //    }
    //}
}

async fn handle_connection_message(connection: &mut Connection, mf: frame::MethodFrameArgs) -> Result<()> {
    Ok(())
}

async fn handle_channel_message(mut channel: Channel, mut rx: mpsc::Receiver<MethodArgs>) -> Result<()> {
    while let Some(_m) = rx.recv().await {
        // TODO if we return Err here, it will be in the JoinHandle
    }

    Ok(())
}
