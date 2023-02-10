use anyhow::Result;
use metalmq_client::*;

#[tokio::main]
async fn main() -> Result<()> {
    let exchange = "test-xchg";
    let queue = "test-queue";

    metalmq_client::setup_logger();

    let mut client = Client::connect("localhost:5672", "guest", "guest").await?;
    let channel = client.channel_open(1).await?;

    channel
        .exchange_declare(exchange, ExchangeType::Fanout, ExchangeDeclareOpts::default())
        .await?;
    channel.queue_declare(queue, QueueDeclareOpts::default()).await?;
    channel
        .queue_bind(queue, exchange, Binding::Direct("".to_string()))
        .await?;

    let message = Message {
        channel: channel.channel,
        body: "Hey man".as_bytes().to_vec(),
        ..Default::default()
    };

    channel
        .basic_publish(exchange, "no-key", message, Mandatory(false), Immediate(false))
        .await?;

    channel.close().await?;
    client.close().await?;

    Ok(())
}
