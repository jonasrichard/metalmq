use super::helper;
use anyhow::Result;
use metalmq_codec::frame::ExchangeDeclareFlags;

#[tokio::test]
async fn declare_exchange() -> Result<()> {
    let mut c = helper::connect().await?;
    let ch = c.channel_open(7).await?;

    ch.exchange_declare("x-new", "direct", None).await?;
    ch.exchange_delete("x-new", false).await?;

    ch.close().await?;
    c.close().await?;

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

    ch2.close().await?;
    c.close().await?;

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

    ch.close().await?;
    c.close().await?;

    Ok(())
}

#[tokio::test]
async fn declare_exchange_with_different_type_error_406() -> Result<()> {
    let mut c = helper::connect().await?;

    let ch = c.channel_open(9).await?;
    ch.exchange_delete("x-conflict", false).await?;
    ch.exchange_declare("x-conflict", "direct", None).await?;

    let result = ch.exchange_declare("x-conflict", "fanout", None).await;

    assert!(result.is_err());

    let err = helper::to_client_error(result);
    assert_eq!(err.channel, Some(9));
    assert_eq!(err.code, 406);
    assert_eq!(err.class_method, metalmq_codec::frame::EXCHANGE_DECLARE);

    // TODO to check if channel is closed because of the exception

    //let che = c.channel_open(9).await?;
    //che.exchange_delete("x-conflict", false).await?;

    Ok(())
}

#[tokio::test]
async fn delete_not_existing_exchange_error_404() -> Result<()> {
    env_logger::builder().is_test(true).try_init();

    let mut c = helper::connect().await?;
    let ch = c.channel_open(9).await?;

    let result = ch.exchange_delete("x-not-existing", false).await;

    assert!(result.is_ok());

    // FIXME here standard says something else, RabbitMQ sends DeleteOk
    //let err = helper::to_client_error(result);
    //assert_eq!(err.channel, Some(9));
    //assert_eq!(err.code, 404);

    ch.close().await?;
    c.close().await?;

    Ok(())
}

#[tokio::test]
async fn delete_used_exchange_if_unused_error_406() -> Result<()> {
    let mut c = helper::connect().await?;
    let ch = c.channel_open(14).await?;
    ch.exchange_delete("x-used", false).await?;

    ch.exchange_declare("x-used", "fanout", None).await?;
    ch.queue_declare("q-used", None).await?;
    ch.queue_bind("q-used", "x-used", "").await?;

    let result = ch.exchange_delete("x-used", true).await;
    assert!(result.is_err());

    let err = helper::to_client_error(result);
    assert_eq!(err.channel, Some(14));
    assert_eq!(err.code, 406);
    assert_eq!(err.class_method, metalmq_codec::frame::EXCHANGE_DELETE);

    ch.close().await?;
    c.close().await?;

    Ok(())
}

#[tokio::test]
async fn auto_delete_exchange_deletes_when_queues_unbound() -> Result<()> {
    let mut c = helper::connect().await?;
    let ch = c.channel_open(99).await?;

    let mut flags = ExchangeDeclareFlags::default();
    flags |= ExchangeDeclareFlags::AUTO_DELETE;
    ch.exchange_declare("x-autodel", "topic", None).await?;
    ch.queue_declare("q-autodel", None).await?;
    ch.queue_bind("q-autodel", "x-autodel", "").await?;

    ch.queue_unbind("q-autodel", "x-autodel", "").await?;
    ch.close().await?;
    c.close().await?;

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    let mut c = helper::connect().await?;
    let ch = c.channel_open(11).await?;

    let mut passive = ExchangeDeclareFlags::default();
    passive |= ExchangeDeclareFlags::PASSIVE;
    let result = ch.exchange_declare("x-autodel", "topic", Some(flags)).await;

    assert!(result.is_err());

    let err = helper::to_client_error(result);
    assert_eq!(err.channel, Some(11));
    assert_eq!(err.code, 404);

    ch.close().await?;
    c.close().await?;

    Ok(())
}
