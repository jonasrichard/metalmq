use metalmq_client::*;

use crate::helper;

async fn exchange_direct_bind(
    channel: &Channel,
    exchange: &str,
    exchange_opts: ExchangeDeclareOpts,
    queue: &str,
    queue_opts: QueueDeclareOpts,
) {
    channel
        .exchange_declare(exchange, ExchangeType::Direct, exchange_opts)
        .await
        .unwrap();

    channel.queue_declare(queue, queue_opts).await.unwrap();

    channel
        .queue_bind(queue, exchange, Binding::Direct("".to_string()))
        .await
        .unwrap();
}

#[tokio::test]
async fn delete_queue_when_last_consumer_left() {
    const EXCHANGE: &str = "x-auto-delete";
    const QUEUE: &str = "q-auto-delete";

    let mut declare = helper::default().connect().await.unwrap();
    let declare_channel = declare.channel_open(3u16).await.unwrap();

    exchange_direct_bind(
        &declare_channel,
        EXCHANGE,
        ExchangeDeclareOpts::default(),
        QUEUE,
        QueueDeclareOpts::default().auto_delete(true),
    )
    .await;

    let mut consumer1 = helper::default().connect().await.unwrap();
    let consumer1_channel = consumer1.channel_open(4u16).await.unwrap();

    let handler1 = consumer1_channel
        .basic_consume(QUEUE, NoAck(false), Exclusive(false), NoLocal(false))
        .await
        .unwrap();

    // If the declaring client terminates, queue won't be deleted until consumer clients are
    // online. But after the last consumer terminates, queue should be deleted.

    declare_channel.close().await.unwrap();
    declare.close().await.unwrap();

    // Check if queue is still there with a passive declare
    let mut checker = helper::default().connect().await.unwrap();
    let checker_channel = checker.channel_open(7u16).await.unwrap();

    checker_channel
        .queue_declare(QUEUE, QueueDeclareOpts::default().passive(true))
        .await
        .unwrap();

    handler1.basic_cancel().await.unwrap();

    let queue_result = checker_channel
        .queue_declare(QUEUE, QueueDeclareOpts::default().passive(true))
        .await;

    assert!(queue_result.is_err());
}
