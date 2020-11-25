use crate::Result;
use ironmq_codec::codec::{AMQPFrame};
use log::info;
use tokio::sync::{mpsc, oneshot};

enum Phase {
    Uninitialized,
    Connected,
    Authenticated,
    ChannelOpened,
    ChannelClosed,
    Closing
}

#[derive(Debug)]
enum Outcome {
    Ok,
    Error(String),
    Frame(AMQPFrame)
}

#[derive(Debug)]
pub(crate) struct Operation {
    input: Option<AMQPFrame>,
    output: Option<oneshot::Sender<Outcome>>
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

        match op.input {
            Some(AMQPFrame::AMQPHeader) =>
                if let Some(out_chan) = op.output {
                    if let Err(_) = out_chan.send(Outcome::Ok) {
                        panic!()
                    }
                },
            None =>
                if let Some(out_chan) = op.output {
                    if let Err(_) = out_chan.send(Outcome::Ok) {
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

    async fn send(input: &mpsc::Sender<Operation>, frame: AMQPFrame) -> oneshot::Receiver<Outcome> {
        let (tx, rx) = oneshot::channel();
        let res = input.send(Operation {
            input: Some(frame),
            output: Some(tx)
        }).await;

        rx
    }

    #[tokio::test]
    async fn connect() {
        let state = start().await.unwrap();
        let rx = send(&state.input, AMQPFrame::AMQPHeader).await;

        let res = rx.await;

        assert!(res.is_ok());
        let ok_outcome = res.map(|outcome| {
            match outcome {
                Outcome::Ok => true,
                _ => false
            }
        });
        //assert!(ok_outcome);
    }
}
