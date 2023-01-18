use crate::helper;
use anyhow::Result;
use metalmq_client::{AutoDelete, Durable, ExchangeType, Immediate, Internal, Mandatory, Passive};

#[tokio::test]
async fn unrouted_mandatory_messages_gives_basic_return() -> Result<()> {
    let mut c = helper::default().connect().await?;
    let ch = c.channel_open(11).await?;

    ch.exchange_declare(
        "x-unrouted",
        ExchangeType::Direct,
        Passive(false),
        Durable(true),
        AutoDelete(false),
        Internal(false),
    )
    .await?;

    ch.basic_publish(
        "x-unrouter",
        "",
        "unrouted message".to_string(),
        Mandatory(false),
        Immediate(false),
    )
    .await;

    Ok(())
}
