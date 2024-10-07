use super::helper;
use anyhow::Result;
use metalmq_client::*;

#[tokio::test]
async fn consume_one_message() -> Result<()> {
    const EXCHANGE: &str = "xchg-consume";
    const QUEUE: &str = "q-consume";

    // Client should not attempt to delete non-existing exchanges and queues -> 404
    //helper::delete_queue(QUEUE).await?;
    //helper::delete_exchange(EXCHANGE).await?;

    let (mut c, _) = helper::default().connect().await?;

    let mut ch = c.channel_open(1).await?;
    helper::declare_exchange_queue(&ch, EXCHANGE, QUEUE).await?;

    let result = helper::consume_messages(&ch, QUEUE, Exclusive(false), 1).await?;

    ch.basic_publish(EXCHANGE, "", PublishedMessage::default().text("Hello"))
        .await?;

    let msgs = result.await.unwrap();
    assert_eq!(msgs.len(), 1);

    let msg = msgs.get(0).unwrap();
    assert_eq!(msg.message.channel, 1);
    assert!(msg.delivery_tag > 0);
    assert_eq!(msg.message.body.len(), 5);
    assert_eq!(msg.message.body, b"Hello");

    ch.close().await?;
    c.close().await?;

    Ok(())
}

#[tokio::test]
async fn consume_not_existing_queue() -> Result<()> {
    let (mut c, _) = helper::default().connect().await?;
    let ch = c.channel_open(2).await?;

    let res = helper::consume_messages(&ch, "not-existing-queue", Exclusive(false), 1).await;

    assert!(res.is_err());

    let err = helper::to_client_error(res);
    assert_eq!(err.channel, Some(2));
    assert_eq!(err.code, 404);
    assert_eq!(err.class_method, metalmq_codec::frame::BASIC_CONSUME);

    Ok(())
}

#[tokio::test]
async fn two_consumers_exclusive_queue_error() -> Result<()> {
    const EXCHANGE: &str = "xchg-exclusive";
    const QUEUE: &str = "q-exclusive";

    helper::delete_queue(QUEUE).await?;
    helper::delete_exchange(EXCHANGE).await?;

    let (mut c, _) = helper::default().connect().await?;

    let ch = c.channel_open(4).await?;

    ch.exchange_declare(
        EXCHANGE,
        ExchangeType::Direct,
        ExchangeDeclareOpts::default().auto_delete(true),
    )
    .await?;

    ch.queue_declare(QUEUE, QueueDeclareOpts::default()).await?;

    ch.queue_bind(QUEUE, EXCHANGE, Binding::Direct("".to_string())).await?;

    let res = helper::consume_messages(&ch, QUEUE, Exclusive(true), 1).await;

    assert!(res.is_ok());

    let (mut c2, _) = helper::default().connect().await?;

    let ch2 = c2.channel_open(3).await?;

    let result = helper::consume_messages(&ch2, QUEUE, Exclusive(true), 1).await;

    println!("{:?}", result);

    assert!(result.is_err());

    let err = helper::to_client_error(result);
    assert_eq!(err.channel, Some(3));
    assert_eq!(err.code, 403);
    assert_eq!(err.class_method, metalmq_codec::frame::BASIC_CONSUME);

    Ok(())
}
