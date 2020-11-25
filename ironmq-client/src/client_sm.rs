use crate::Result;
use ironmq_codec::codec::{AMQPFrame};
use log::info;
use tokio::sync::{mpsc, oneshot};

pub(crate) enum Phase {
    Uninitialized,
    Connected,
    Authenticated,
    ChannelOpened,
    ChannelClosed,
    Closing
}

#[derive(Debug)]
pub(crate) struct Operation {
    input_frame: Option<AMQPFrame>,
    output: Option<oneshot::Sender<bool>>
    // here probably we need a oneshot channel to give back control to the caller
}

pub(crate) struct ClientState {
    input: mpsc::Sender<Operation>
}

pub(crate) async fn start() -> Result<ClientState> {
    let (sender, mut receiver) = mpsc::channel(16);

    tokio::spawn(async move {
        println!("Starting process loop");
        process_input(&mut receiver).await
    });

    Ok(ClientState {
        input: sender
    })
}

async fn process_input(input: &mut mpsc::Receiver<Operation>) -> Result<()> {
    while let Some(op) = input.recv().await {
        println!("Got {:?}", op);

        match op.input_frame {
            Some(AMQPFrame::AMQPHeader) =>
                if let Some(out_chan) = op.output {
                    if let Err(_) = out_chan.send(true) {
                        panic!()
                    }
                },
            None =>
                if let Some(out_chan) = op.output {
                    if let Err(_) = out_chan.send(true) {
                        panic!()
                    }
                },
            _ =>
                ()
        }
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn connect() {
        let state = start().await.unwrap();
        let (tx, rx) = oneshot::channel();

        let res = state.input.send(Operation {
            input_frame: None,
            output: Some(tx)
        }).await;

        assert!(res.is_ok());

        let result = rx.await;
        assert_eq!(Ok(true), result);
    }
}
