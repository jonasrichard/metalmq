//! Utility module for receiving messages from channels with timeout.
use metalmq_codec::{codec::Frame, frame::AMQPFrame};
use tokio::sync::mpsc;

use crate::error::RuntimeError;

/// Receiving with timeout
pub async fn recv_with_timeout<T>(rx: &mut mpsc::Receiver<T>) -> Option<T> {
    let sleep = tokio::time::sleep(tokio::time::Duration::from_millis(100));
    tokio::pin!(sleep);

    tokio::select! {
        frame = rx.recv() => {
            frame
        }
        _ = &mut sleep => {
            None
        }
    }
}

/// Receive a single frame from the channel and panics if there is a timeout or we received more
/// frames. See [`Frame`] enum.
pub async fn recv_single_frame(rx: &mut mpsc::Receiver<Frame>) -> AMQPFrame {
    debug_assert!(!rx.is_closed());

    let f = recv_with_timeout(rx).await.expect("No response is received");

    match f {
        Frame::Frame(sf) => sf,
        Frame::Frames(_) => panic!("Multiple frames are received"),
    }
}

/// Receive multiple frames from the channel and panics if there is a timeout or we received
/// only a single frame. See [`Frame`] enum.
pub async fn recv_multiple_frames(rx: &mut mpsc::Receiver<Frame>) -> Vec<AMQPFrame> {
    debug_assert!(!rx.is_closed());

    let f = recv_with_timeout(rx).await.expect("No response is received");

    match f {
        Frame::Frame(_) => panic!("A single frame is received"),
        Frame::Frames(mf) => mf,
    }
}

/// Listens for messages in the channel and if it doesn't get any, returns true.
pub async fn recv_nothing<T>(rx: &mut mpsc::Receiver<T>) -> bool {
    debug_assert!(!rx.is_closed());

    recv_with_timeout(rx).await.is_none()
}

/// Listens for frames in the channel and returns the [`RuntimeError`] as the error frame. If it
/// doesn't get anything or a non-error frame, it panics.
pub async fn recv_error_frame(rx: &mut mpsc::Receiver<Frame>) -> RuntimeError {
    debug_assert!(!rx.is_closed());

    let f = recv_single_frame(rx).await;

    f.into()
}
