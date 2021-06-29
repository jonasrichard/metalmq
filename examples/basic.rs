use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let exchange = "test-xchg";
    let queue = "test-queue";

    metalmq_client::setup_logger();

    let mut client = metalmq_client::connect("localhost:5672", "guest", "guest").await?;
    client.open("/").await?;
    let channel = client.channel_open(1).await?;

    channel.exchange_declare(exchange, "fanout", None).await?;
    channel.queue_declare(queue).await?;
    channel.queue_bind(queue, exchange, "").await?;

    channel.basic_publish(exchange, "no-key", "Hey man".into()).await?;

    channel.close().await?;
    client.close().await?;

    Ok(())
}
