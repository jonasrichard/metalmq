use env_logger::Builder;
use std::io::Write;

pub(crate) async fn send_timeout<T: std::fmt::Debug>(
    ms: &tokio::sync::mpsc::Sender<T>,
    value: T,
) -> Result<(), tokio::sync::mpsc::error::SendError<T>> {
    let timeout = tokio::time::sleep(tokio::time::Duration::from_secs(1));
    tokio::pin!(timeout);

    log::trace!("Send with timeout {:?}", value);

    tokio::select! {
        result = ms.send(value) => {
            result
        }
        _ = &mut timeout => {
            panic!("Timeout during sending to channel");
        }
    }
}

/// Convenience function for setting up `env_logger` to see log messages.
pub fn setup_logger() {
    let mut builder = Builder::from_default_env();

    builder
        .format_timestamp_millis()
        .format(|buf, record| {
            let mut lvl = buf.style();
            lvl.set_bold(true);

            match record.level() {
                log::Level::Error => lvl.set_color(env_logger::fmt::Color::Red),
                log::Level::Warn => lvl.set_color(env_logger::fmt::Color::Yellow),
                log::Level::Info => lvl.set_color(env_logger::fmt::Color::Green),
                log::Level::Debug => lvl.set_color(env_logger::fmt::Color::Rgb(160, 160, 160)),
                log::Level::Trace => lvl.set_color(env_logger::fmt::Color::Rgb(96, 96, 96)),
            };

            writeln!(
                buf,
                "{} - [{:5}] {}:{} - {}",
                buf.timestamp_millis(),
                lvl.value(record.level()),
                record.file().unwrap_or_default(),
                record.line().unwrap_or_default(),
                record.args()
            )
        })
        .write_style(env_logger::WriteStyle::Always)
        .init();
}
