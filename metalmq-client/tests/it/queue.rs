use super::helper;
use anyhow::Result;
use metalmq_client::{AutoDelete, Durable, ExchangeType, Exclusive, IfEmpty, IfUnused, Internal, Passive};

#[tokio::test]
async fn direct_exchange_queue_bind() -> Result<()> {
    let mut c = helper::connect().await?;
    let ch = c.channel_open(18).await?;

    ch.exchange_declare(
        "prices",
        ExchangeType::Direct,
        Passive(false),
        Durable(true),
        AutoDelete(false),
        Internal(false),
    )
    .await?;
    ch.queue_declare(
        "price-queue",
        Passive(false),
        Durable(true),
        Exclusive(false),
        AutoDelete(false),
    )
    .await?;
    ch.queue_bind("price-queue", "prices", "").await?;

    ch.exchange_delete("prices", IfUnused(false)).await?;
    ch.queue_delete("price-queue", IfUnused(false), IfEmpty(false)).await?;

    ch.close().await?;
    c.close().await?;

    Ok(())
}
