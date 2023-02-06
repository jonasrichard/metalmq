use super::helper;
use anyhow::Result;
use metalmq_client::{ExchangeDeclareOpts, ExchangeType, IfUnused, QueueDeclareOpts};

#[tokio::test]
async fn declare_exchange() -> Result<()> {
    let mut c = helper::connect().await?;
    let ch = c.channel_open(7).await?;

    ch.exchange_declare(
        "x-new",
        ExchangeType::Direct,
        ExchangeDeclareOpts::default().durable(true),
    )
    .await?;
    ch.exchange_delete("x-new", IfUnused(false)).await?;

    ch.close().await?;
    c.close().await?;

    Ok(())
}

#[tokio::test]
async fn passive_declare_existing_exchange() -> Result<()> {
    let mut c = helper::connect().await?;

    let ch = c.channel_open(7).await?;
    ch.exchange_declare(
        "x-passive",
        ExchangeType::Direct,
        ExchangeDeclareOpts::default().durable(true),
    )
    .await?;
    ch.close().await?;

    let ch2 = c.channel_open(8).await?;
    ch2.exchange_declare(
        "x-passive",
        ExchangeType::Direct,
        ExchangeDeclareOpts::default().passive(true).durable(true),
    )
    .await?;

    ch2.exchange_delete("x-passive", IfUnused(false)).await?;

    ch2.close().await?;
    c.close().await?;

    Ok(())
}

#[tokio::test]
async fn create_exchange_after_delete_the_old() -> Result<()> {
    let mut c = helper::connect().await?;

    let ch = c.channel_open(7).await?;
    ch.exchange_declare(
        "x-del-test",
        ExchangeType::Direct,
        ExchangeDeclareOpts::default().durable(true),
    )
    .await?;
    ch.exchange_delete("x-del-test", IfUnused(false)).await?;
    ch.exchange_declare(
        "x-del-test",
        ExchangeType::Fanout,
        ExchangeDeclareOpts::default().durable(true),
    )
    .await?;

    ch.exchange_delete("x-del-test", IfUnused(false)).await?;

    ch.close().await?;
    c.close().await?;

    Ok(())
}

#[tokio::test]
async fn declare_exchange_with_different_type_error_406() -> Result<()> {
    let mut c = helper::connect().await?;

    let ch = c.channel_open(9).await?;
    //ch.exchange_delete("x-conflict", false).await?;
    ch.exchange_declare(
        "x-conflict",
        ExchangeType::Direct,
        ExchangeDeclareOpts::default().durable(true),
    )
    .await?;

    let result = ch
        .exchange_declare(
            "x-conflict",
            ExchangeType::Fanout,
            ExchangeDeclareOpts::default().durable(true),
        )
        .await;

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
    env_logger::builder().is_test(true).try_init().unwrap();

    let mut c = helper::connect().await?;
    let ch = c.channel_open(9).await?;

    let result = ch.exchange_delete("x-not-existing", IfUnused(false)).await;

    assert!(result.is_err());

    let err = helper::to_client_error(result);
    assert_eq!(err.channel, Some(9));
    assert_eq!(err.code, 404);

    ch.close().await?;
    c.close().await?;

    Ok(())
}

#[tokio::test]
async fn delete_used_exchange_if_unused_error_406() -> Result<()> {
    let mut c = helper::connect().await?;
    let ch = c.channel_open(14).await?;
    //ch.exchange_delete("x-used", false).await?;

    ch.exchange_declare(
        "x-used",
        ExchangeType::Fanout,
        ExchangeDeclareOpts::default().durable(true),
    )
    .await?;
    ch.queue_declare("q-used", QueueDeclareOpts::default()).await?;
    ch.queue_bind("q-used", "x-used", "").await?;

    let result = ch.exchange_delete("x-used", IfUnused(true)).await;
    assert!(result.is_err());

    let err = helper::to_client_error(result);
    assert_eq!(err.channel, Some(14));
    assert_eq!(err.code, 406);
    assert_eq!(err.class_method, metalmq_codec::frame::EXCHANGE_DELETE);

    ch.close().await?;
    c.close().await?;

    Ok(())
}

#[ignore]
#[tokio::test]
async fn auto_delete_exchange_deletes_when_queues_unbound() -> Result<()> {
    let mut c = helper::connect().await?;
    let ch = c.channel_open(99).await?;

    ch.exchange_declare(
        "x-autodel",
        ExchangeType::Topic,
        ExchangeDeclareOpts::default().durable(true).auto_delete(true),
    )
    .await?;
    ch.queue_declare("q-autodel", QueueDeclareOpts::default()).await?;
    ch.queue_bind("q-autodel", "x-autodel", "").await?;

    ch.queue_unbind("q-autodel", "x-autodel", "").await?;
    ch.close().await?;
    c.close().await?;

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    let mut c = helper::connect().await?;
    let ch = c.channel_open(11).await?;

    let result = ch
        .exchange_declare(
            "x-autodel",
            ExchangeType::Topic,
            ExchangeDeclareOpts::default().passive(true).durable(true),
        )
        .await;

    assert!(result.is_err());

    let err = helper::to_client_error(result);
    assert_eq!(err.channel, Some(11));
    assert_eq!(err.code, 406);

    ch.close().await?;
    c.close().await?;

    Ok(())
}
