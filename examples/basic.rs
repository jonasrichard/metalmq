use anyhow::Result;
use metalmq_client::ExchangeType;

#[tokio::main]
async fn main() -> Result<()> {
    let exchange = "test-xchg";
    let queue = "test-queue";

    metalmq_client::setup_logger();

    let mut client = metalmq_client::connect("localhost:5672", "guest", "guest").await?;
    let channel = client.channel_open(1).await?;

    channel.exchange_declare(exchange, ExchangeType::Fanout, None).await?;
    channel.queue_declare(queue, None).await?;
    channel.queue_bind(queue, exchange, "").await?;

    channel
        .basic_publish(exchange, "no-key", "Hey man".into(), false, false)
        .await?;

    channel.close().await?;
    client.close().await?;

    Ok(())
}
