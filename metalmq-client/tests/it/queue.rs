use super::helper;
use anyhow::Result;
use metalmq_client::{ExchangeType, IfEmpty, IfUnused};

#[tokio::test]
async fn direct_exchange_queue_bind() -> Result<()> {
    let mut c = helper::connect().await?;
    let ch = c.channel_open(18).await?;

    ch.exchange_declare("prices", ExchangeType::Direct, None).await?;
    ch.queue_declare("price-queue", None).await?;
    ch.queue_bind("price-queue", "prices", "").await?;

    ch.exchange_delete("prices", IfUnused(false)).await?;
    ch.queue_delete("price-queue", IfUnused(false), IfEmpty(false)).await?;

    ch.close().await?;
    c.close().await?;

    Ok(())
}
