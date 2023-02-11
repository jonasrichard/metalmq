pub(crate) async fn send_timeout<T: std::fmt::Debug>(
    ms: &tokio::sync::mpsc::Sender<T>,
    value: T,
) -> Result<(), tokio::sync::mpsc::error::SendError<T>> {
    let timeout = tokio::time::sleep(tokio::time::Duration::from_secs(1));
    tokio::pin!(timeout);

    tokio::select! {
        result = ms.send(value) => {
            result
        }
        _ = &mut timeout => {
            panic!("Timeout during sending to channel");
        }
    }
}
