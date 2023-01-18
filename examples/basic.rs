use anyhow::Result;
use metalmq_client::{AutoDelete, Client, Durable, ExchangeType, Exclusive, Immediate, Internal, Mandatory, Passive};

#[tokio::main]
async fn main() -> Result<()> {
    let exchange = "test-xchg";
    let queue = "test-queue";

    metalmq_client::setup_logger();

    let mut client = Client::connect("localhost:5672", "guest", "guest").await?;
    let channel = client.channel_open(1).await?;

    channel
        .exchange_declare(
            exchange,
            ExchangeType::Fanout,
            Passive(false),
            Durable(false),
            AutoDelete(false),
            Internal(false),
        )
        .await?;
    channel
        .queue_declare(
            queue,
            Passive(false),
            Durable(false),
            Exclusive(false),
            AutoDelete(false),
        )
        .await?;
    channel.queue_bind(queue, exchange, "").await?;

    channel
        .basic_publish(exchange, "no-key", "Hey man".into(), Mandatory(false), Immediate(false))
        .await?;

    channel.close().await?;
    client.close().await?;

    Ok(())
}
