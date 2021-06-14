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

    let (otx, orx) = oneshot::channel();

    let (tx, mut rx) = mpsc::channel(1);
    tokio::spawn(async move {
        let mut count = 0;

        info!("Waiting for incoming messages...");

        while let Some(msg) = rx.recv().await {
            info!("{:?}", msg);
            count += 1;
            if count == 1 {
                break;
            }
        }
        otx.send(()).unwrap();
    });

    client.basic_consume(1, queue, "ctag", tx).await?;
    client.basic_publish(1, exchange, "", "Hey man".into()).await?;

    orx.await.unwrap();

    client.channel_close(1).await?;
    client.close().await?;

    Ok(())
}
