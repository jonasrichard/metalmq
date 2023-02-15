use super::helper;
use anyhow::Result;
use metalmq_client::{Binding, ExchangeDeclareOpts, ExchangeType, IfEmpty, IfUnused, QueueDeclareOpts};

#[tokio::test]
async fn direct_exchange_queue_bind_and_delete() -> Result<()> {
    let (mut c, _) = helper::connect().await?;
    let mut ch = c.channel_open(18).await?;

    ch.exchange_declare(
        "prices",
        ExchangeType::Direct,
        ExchangeDeclareOpts::default().durable(true),
    )
    .await?;
    ch.queue_declare("price-queue", QueueDeclareOpts::default().durable(true))
        .await?;
    ch.queue_bind("price-queue", "prices", Binding::Direct("".to_string()))
        .await?;

    ch.queue_unbind("price-queue", "prices", "").await?;
    ch.exchange_delete("prices", IfUnused(false)).await?;
    ch.queue_delete("price-queue", IfUnused(false), IfEmpty(false)).await?;

    ch.close().await?;
    c.close().await?;

    Ok(())
}
