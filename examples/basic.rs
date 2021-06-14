use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let exchange = "test-xchg";
    let queue = "test-queue";

    metalmq_client::setup_logger();

    let client = metalmq_client::connect("localhost:5672", "guest", "guest").await?;
    client.open("/").await?;
    client.channel_open(1).await?;

    client.exchange_declare(1, exchange, "fanout", None).await?;
    client.queue_declare(1, queue).await?;
    client.queue_bind(1, queue, exchange, "").await?;

    client.basic_publish(1, exchange, "no-key", "Hey man".into()).await?;

    client.channel_close(1).await?;
    client.close().await?;

    Ok(())
}
