use anyhow::Result;
use log::{error, info};
use std::time::Instant;
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

    let message_count = 1024u32;

    let (otx, orx) = oneshot::channel();

    let (tx, mut rx) = mpsc::channel::<metalmq_client::Message>(512);
    let consumer = channel.consumer();

    tokio::spawn(async move {
        let mut count = 0u32;

        //info!("Waiting for incoming messages...");

        while let Some(msg) = rx.recv().await {
            //info!("count = {}, delivery_tag = {}", count, msg.delivery_tag);

            // FIXME here we have a deadlock. When we are waiting here, the client_sm
            // wants to send a message to this rx channel and we are waiting here, so
            // it also blocks.
            if let Err(e) = consumer.basic_ack(msg.delivery_tag).await {
                error!("Error during sending basic.ack {:?}", e);
            }

            count += 1;
            if count == message_count {
                break;
            }
        }
        otx.send(()).unwrap();
    });

    channel.basic_consume(queue, "ctag", tx).await?;

    let message = "This will be the test message what we send over multiple times";

    let start = Instant::now();

    for _ in 0..message_count {
        channel.basic_publish(exchange, "", message.to_string()).await?;
    }

    orx.await.unwrap();

    println!(
        "Send and receive {} messages: {:?}",
        message_count,
        Instant::elapsed(&start)
    );

    channel.close().await?;
    client.close().await?;

    Ok(())
}
