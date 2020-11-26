use crate::Result;
use ironmq_codec::codec::{AMQPFrame, AMQPValue};
use ironmq_codec::frame;
use log::info;
use tokio::sync::{mpsc, oneshot};

pub(crate) struct ClientState {
}

pub(crate) trait Client {
    fn connection_start(&mut self, properties: ServerProperties) -> Result<ClientProperties>;
    fn connection_tune(&mut self, tune_properties: TuneProperties) -> Result<TuneProperties>;
}

struct ServerProperties {
    channel: u16,
    properties: Vec<(String, AMQPValue)>
}

struct ClientProperties {
}

struct TuneProperties {
}

impl Client for ClientState {
    fn connection_start(&mut self, properties: ServerProperties) -> Result<ClientProperties> {
        Ok(ClientProperties {
        })
    }

    fn connection_tune(&mut self, tune_properties: TuneProperties) -> Result<TuneProperties> {
        Ok(TuneProperties {
        })
    }
}

impl From<AMQPFrame> for ServerProperties {
    fn from(fr: AMQPFrame) -> ServerProperties {
        match fr {
            AMQPFrame::Method(channel, cm, args) =>
                ServerProperties {
                    channel: channel,
                    properties: vec![]
                },
            _ =>
                panic!("Conversion error")
        }
    }
}

impl From<ClientProperties> for AMQPFrame {
    fn from(cp: ClientProperties) -> AMQPFrame {
        AMQPFrame::Method(0, 0, Box::new(vec![]))
    }
}

impl From<AMQPFrame> for TuneProperties {
    fn from(fr: AMQPFrame) -> TuneProperties {
        match fr {
            AMQPFrame::Method(channel, cm, args) =>
                TuneProperties {
                },
            _ =>
                panic!("Conversion error")
        }
    }
}

impl From<TuneProperties> for AMQPFrame {
    fn from(tp: TuneProperties) -> AMQPFrame {
        AMQPFrame::Method(0, 0, Box::new(vec![]))
    }
}



enum Phase {
    Uninitialized,
    Connected,
    Authenticated,
    ChannelOpened,
    ChannelClosed,
    Closing
}

#[derive(Debug)]
pub(crate) enum Outcome {
    Ok,
    Error(String),
    Frame(AMQPFrame)
}

#[derive(Debug)]
pub(crate) struct Operation {
    pub(crate) input: AMQPFrame,
    pub(crate) output: Option<oneshot::Sender<Outcome>>
}

pub(crate) struct ClientState2 {
    pub(crate) input: mpsc::Sender<Operation>
}

// TODO this module shouldn't be an async automata, it should be a sync
// mutable state which validates the client logic

pub(crate) async fn start() -> Result<ClientState> {
    let (sender, mut receiver) = mpsc::channel(16);

    tokio::spawn(async move {
        process_input(&mut receiver).await
    });

    Ok(ClientState {
        input: sender
    })
}

async fn process_input(input: &mut mpsc::Receiver<Operation>) -> Result<()> {
    info!("Processing operations...");

    while let Some(op) = input.recv().await {
        info!("Operation: {:?}", op);

        match op.input {
            AMQPFrame::AMQPHeader =>
                send_feedback(op.output, Outcome::Ok).await?,
            AMQPFrame::Method(channel, cm, args) =>
                match reply_to_method_frame(channel, cm, args.to_vec()) {
                    Some(frame) =>
                        send_feedback(op.output, Outcome::Frame(frame)).await?,
                    None =>
                        ()
                },
            _ =>
                ()
        }
    }
    Ok(())
}

/// Send back feedback if it is possible
async fn send_feedback(output: Option<oneshot::Sender<Outcome>>, outcome: Outcome) -> Result<()> {
    match output {
        Some(chan) =>
            if let Err(_) = chan.send(outcome) {
                Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Cannot send feedback")))
            } else {
                Ok(())
            },
        None =>
            Ok(())
    }
}

fn reply_to_method_frame(channel: u16, cm: u32, args: Vec<AMQPValue>) -> Option<AMQPFrame> {
    match cm {
        frame::CONNECTION_OPEN =>
            Some(frame::connection_open_ok(channel)),
        frame::CONNECTION_START =>
            Some(reply_to_connection_start(channel, args)),
        frame::CONNECTION_TUNE =>
            Some(frame::connection_tune_ok(channel)),
        _ =>
            None
    }
}

fn reply_to_connection_start(channel: u16, _args: Vec<AMQPValue>) -> AMQPFrame {
    frame::connection_start_ok(channel)
}

#[cfg(test)]
mod test {
    use super::*;

    async fn send(input: &mpsc::Sender<Operation>, frame: AMQPFrame) -> oneshot::Receiver<Outcome> {
        let (tx, rx) = oneshot::channel();
        let res = input.send(Operation {
            input: frame,
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

        assert!(ok_outcome.unwrap());
    }
}
