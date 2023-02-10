use crate::{helper, message_from_string};
use anyhow::Result;
use metalmq_client::{ExchangeDeclareOpts, ExchangeType, Immediate, Mandatory};

#[tokio::test]
async fn unrouted_mandatory_messages_gives_basic_return() -> Result<()> {
    let mut c = helper::default().connect().await?;
    let ch = c.channel_open(11).await?;

    ch.exchange_declare(
        "x-unrouted",
        ExchangeType::Direct,
        ExchangeDeclareOpts::default().durable(true),
    )
    .await?;

    ch.basic_publish(
        "x-unrouter",
        "",
        message_from_string(11, "unrouted message".to_string()),
        Mandatory(false),
        Immediate(false),
    )
    .await;

    Ok(())
}
