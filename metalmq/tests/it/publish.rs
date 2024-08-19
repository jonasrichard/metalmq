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

// basic publish without exchange name but routing key = queue name should direct the
// message to that queue
// aka default exchange

#[tokio::test]
async fn message_to_default_exchange_go_to_queue_by_routing_key() -> Result<()> {
    let (mut c, _) = helper::default().connect().await?;
    let mut ch = c.channel_open(15).await?;

    ch.queue_declare("q-def-exchange", QueueDeclareOpts::default().durable(true))
        .await?;

    let mut ch_consume = c.channel_open(16).await?;

    let result = helper::consume_messages(&ch_consume, "q-def-exchange", Exclusive(false), 1).await?;

    ch.basic_publish("", "q-def-exchange", "Message to default exchange")
        .await?;

    let msgs = result.await.unwrap();

    let msg = msgs.first().unwrap();

    assert_eq!("", msg.exchange);
    assert_eq!(b"Message to default exchange", msg.message.body.as_slice());

    ch_consume.close().await?;
    ch.close().await?;
    c.close().await?;

    Ok(())
}
