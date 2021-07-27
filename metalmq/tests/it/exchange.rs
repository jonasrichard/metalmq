use super::helper;
use anyhow::Result;

#[tokio::test]
async fn passive_exchange_existing_exchange() -> Result<()> {
    let mut c = helper::default().connect().await?;
    let ch = c.channel_open(11).await?;

    ch.exchange_declare("xchg-existing", "fanout", None).await?;

    let mut flags = metalmq_codec::frame::ExchangeDeclareFlags::empty();
    flags |= metalmq_codec::frame::ExchangeDeclareFlags::PASSIVE;

    let result = ch.exchange_declare("xcgh-existing", "fanout", Some(flags)).await;

    assert!(result.is_err());

    Ok(())
}
