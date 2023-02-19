use crate::helper;
use anyhow::Result;
use metalmq_client::*;

#[tokio::test]
async fn unrouted_mandatory_messages_gives_basic_return() -> Result<()> {
    let (mut c, _) = helper::default().connect().await?;
    let ch = c.channel_open(11).await?;

    ch.exchange_declare(
        "x-unrouted",
        ExchangeType::Direct,
        ExchangeDeclareOpts::default().durable(true),
    )
    .await?;

    ch.basic_publish("x-unrouter", "", PublishedMessage::default().text("unrouted message"))
        .await
        .unwrap();

    Ok(())
}
