use anyhow::Result;
use log::info;
use tokio::sync::{mpsc, oneshot};

#[tokio::main]
async fn main() -> Result<()> {
    let exchange = "x_pubsub";
    let queue = "q_pubsub";

    metalmq_client::setup_logger();

    let mut client = metalmq_client::connect("localhost:5672", "guest", "guest").await?;
    client.open("/").await?;

    let channel = client.channel_open(1).await?;

    channel.exchange_declare(exchange, "direct", None).await?;
    channel.queue_declare(queue).await?;
    channel.queue_bind(queue, exchange, "").await?;

    let message_count = 10u32;

    let (otx, orx) = oneshot::channel();

    let (tx, mut rx) = mpsc::channel::<metalmq_client::Message>(1);
    let consumer = channel.consumer();

    tokio::spawn(async move {
        let mut count = 0u32;

        info!("Waiting for incoming messages...");

        while let Some(msg) = rx.recv().await {
            info!("{:?}", msg);
            consumer.basic_ack(msg.delivery_tag).await;

            count += 1;
            if count == message_count {
                break;
            }
        }
        otx.send(()).unwrap();
    });

    channel.basic_consume(queue, "ctag", tx).await?;

    let message = "This will be the test message what we send over multiple times";

    for _ in 0..message_count {
        channel.basic_publish(exchange, "", message.to_string()).await?;
    }

    orx.await.unwrap();

    channel.close().await?;
    client.close().await?;

    Ok(())
}
