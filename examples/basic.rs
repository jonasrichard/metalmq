use anyhow::Result;
use metalmq_client::*;

#[tokio::main]
async fn main() -> Result<()> {
    let exchange = "test-xchg";
    let queue = "test-queue";

    let mut client = Client::connect("localhost:5672", "guest", "guest").await?;
    let mut channel = client.channel_open_next().await?;

    channel
        .exchange_declare(exchange, ExchangeType::Fanout, ExchangeDeclareOpts::default())
        .await?;
    channel.queue_declare(queue, QueueDeclareOpts::default()).await?;
    channel
        .queue_bind(queue, exchange, Binding::Direct("".to_string()))
        .await?;

    let message = PublishedMessage::default().str("Hey man");

    channel.basic_publish(exchange, "no-key", message).await?;

    channel.close().await?;
    client.close().await?;

    Ok(())
}
