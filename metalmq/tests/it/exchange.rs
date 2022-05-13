use super::helper;
use anyhow::Result;
use metalmq_client::ExchangeType;

#[tokio::test]
async fn passive_exchange_existing_exchange() -> Result<()> {
    let mut c = helper::default().connect().await?;
    let ch = c.channel_open(11).await?;

    ch.exchange_declare("xchg-existing", ExchangeType::Fanout, None).await?;

    let mut flags = metalmq_codec::frame::ExchangeDeclareFlags::empty();
    flags |= metalmq_codec::frame::ExchangeDeclareFlags::PASSIVE;

    let result = ch
        .exchange_declare("xcgh-existing", ExchangeType::Fanout, Some(flags))
        .await;

    assert!(result.is_err());

    Ok(())
}

#[tokio::test]
async fn two_connections_publishing_to_the_same_exchange() -> Result<()> {
    let mut c1 = helper::default().connect().await?;
    let mut c2 = helper::default().connect().await?;
    let ch1 = c1.channel_open(32).await?;
    let ch2 = c2.channel_open(31).await?;

    ch1.exchange_declare("xchg-shared", ExchangeType::Direct, None).await?;

    ch1.basic_publish("xcgh-shared", "", "Content".to_string(), false, false)
        .await?;
    ch2.basic_publish("xcgh-shared", "", "Content".to_string(), false, false)
        .await?;

    ch1.close().await?;
    ch2.close().await?;

    c1.close().await?;
    c2.close().await?;

    Ok(())
}
