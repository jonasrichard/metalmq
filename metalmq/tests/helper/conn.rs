use anyhow::Result;
use metalmq_client::*;
use metalmq_codec::frame::ExchangeDeclareFlags;
use tokio::sync::{mpsc, oneshot};

pub(crate) async fn default_connection(exchange: &str, queue: &str) -> Result<Client> {
    let c = connect("127.0.0.1:5672", "guest", "guest").await?;
    c.open("/").await?;
    c.channel_open(1).await?;

    let mut ex_flags = ExchangeDeclareFlags::empty();
    ex_flags |= ExchangeDeclareFlags::AUTO_DELETE;
    c.exchange_declare(1, exchange, "fanout", Some(ex_flags)).await?;
    c.queue_declare(1, queue).await?;

    c.queue_bind(1, queue, exchange, "").await?;

    Ok(c)
}

fn to_client_error<T: std::fmt::Debug>(result: Result<T>) -> ClientError {
    result.unwrap_err().downcast::<ClientError>().unwrap()
}

pub(crate) async fn consume_messages<'a>(
    client: &'a Client,
    channel: Channel,
    queue: &'a str,
    ctag: &'a str,
    tx: oneshot::Sender<Vec<Message>>,
    n: usize,
) -> Result<()> {
    let (sink, mut source) = mpsc::channel(1);

    tokio::spawn(async move {
        let mut messages = vec![];

        while let Some(msg) = source.recv().await {
            messages.push(msg);

            if messages.len() == n {
                break;
            }
        }

        tx.send(messages).unwrap();
    });

    client.basic_consume(channel, queue, ctag, sink).await?;

    Ok(())
}
