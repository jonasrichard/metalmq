use anyhow::Result;
use log::info;
use tokio::sync::{mpsc, oneshot};

#[tokio::main]
async fn main() -> Result<()> {
    let exchange = "x_pubsub";
    let queue = "q_pubsub";

    metalmq_client::setup_logger();

    let client = metalmq_client::connect("localhost:5672", "guest", "guest").await?;
    client.open("/").await?;
    client.channel_open(1).await?;

    client.exchange_declare(1, exchange, "fanout", None).await?;
    client.queue_declare(1, queue).await?;
    client.queue_bind(1, queue, exchange, "").await?;

    let message_count = 10;

    let (otx, orx) = oneshot::channel();

    let (tx, mut rx) = mpsc::channel(1);
    tokio::spawn(async move {
        let mut count = 0;

        info!("Waiting for incoming messages...");

        while let Some(msg) = rx.recv().await {
            info!("{:?}", msg);
            // FIXME here I cannot move client because we have only one
            count += 1;
            if count == message_count {
                break;
            }
        }
        otx.send(()).unwrap();
    });

    client.basic_consume(1, queue, "ctag", tx).await?;

    let message = "This will be the test message what we send over multiple times";

    for _ in 0..message_count {
        client.basic_publish(1, exchange, "", message.to_string()).await?;
    }

    orx.await.unwrap();

    client.channel_close(1).await?;
    client.close().await?;

    Ok(())
}
