use super::helper;
use anyhow::Result;
use metalmq_codec::frame::ExchangeDeclareFlags;

#[tokio::test]
async fn declare_exchange() -> Result<()> {
    let mut c = helper::connect().await?;
    let ch = c.channel_open(7).await?;

    ch.exchange_declare("x-new", "direct", None).await?;

    ch.exchange_delete("x-new", false).await?;

    Ok(())
}

#[tokio::test]
async fn passive_declare_existing_exchange() -> Result<()> {
    let mut c = helper::connect().await?;

    let ch = c.channel_open(7).await?;
    ch.exchange_declare("x-passive", "direct", None).await?;
    ch.close().await?;

    let ch2 = c.channel_open(8).await?;
    let mut flags = ExchangeDeclareFlags::default();
    flags |= ExchangeDeclareFlags::PASSIVE;
    ch2.exchange_declare("x-passive", "direct", Some(flags)).await?;

    ch2.exchange_delete("x-passive", false).await?;

    Ok(())
}

#[tokio::test]
async fn create_exchange_after_delete_the_old() -> Result<()> {
    let mut c = helper::connect().await?;

    let ch = c.channel_open(7).await?;
    ch.exchange_declare("x-del-test", "direct", None).await?;
    ch.exchange_delete("x-del-test", false).await?;
    ch.exchange_declare("x-del-test", "fanout", None).await?;

    ch.exchange_delete("x-del-test", false).await?;

    Ok(())
}

#[tokio::test]
async fn declare_exchange_with_different_type() -> Result<()> {
    let mut c = helper::connect().await?;

    let ch = c.channel_open(9).await?;
    ch.exchange_declare("x-conflict", "direct", None).await?;

    let result = ch.exchange_declare("x-conflict", "fanout", None).await;

    assert!(result.is_err());

    let err = helper::to_client_error(result);
    assert_eq!(err.channel, Some(9));
    assert_eq!(err.code, 406);
    assert_eq!(err.class_method, metalmq_codec::frame::EXCHANGE_DECLARE);

    // TODO to check if channel is closed because of the exception

    let che = c.channel_open(9).await?;
    che.exchange_delete("x-conflict", false).await?;

    Ok(())
}
