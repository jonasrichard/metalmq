use std::time::Duration;

use crate::helper;
use metalmq_client::*;

#[tokio::test]
async fn test_routing_logic() {
    let (mut client, _) = helper::connect().await.unwrap();
    let mut channel = client.channel_open(2u16).await.unwrap();

    channel
        .exchange_declare("event-hub", ExchangeType::Fanout, ExchangeDeclareOpts::default())
        .await
        .unwrap();
    channel
        .queue_declare("consumer-view-1", QueueDeclareOpts::default())
        .await
        .unwrap();
    channel
        .queue_declare("consumer-view-2", QueueDeclareOpts::default())
        .await
        .unwrap();
    channel
        .queue_bind("consumer-view-1", "event-hub", Binding::Fanout)
        .await
        .unwrap();
    channel
        .queue_bind("consumer-view-2", "event-hub", Binding::Fanout)
        .await
        .unwrap();

    publish_deliver_message_to_all_queues(&mut client).await;

    channel
        .queue_delete("consumer-view-1", IfUnused(false), IfEmpty(false))
        .await
        .unwrap();
    channel
        .queue_delete("consumer-view-2", IfUnused(false), IfEmpty(false))
        .await
        .unwrap();
    channel.queue_unbind("consumer-view-1", "event-hub", "").await.unwrap();
    channel.queue_unbind("consumer-view-2", "event-hub", "").await.unwrap();
    channel.exchange_delete("event-hub", IfUnused(false)).await.unwrap();

    channel.close().await.unwrap();
    client.close().await.unwrap();
}

async fn publish_deliver_message_to_all_queues(client: &mut Client) {
    let mut channel1 = client.channel_open(3u16).await.unwrap();
    let mut consumer1 = channel1
        .basic_consume("consumer-view-1", NoAck(false), Exclusive(false), NoLocal(false))
        .await
        .unwrap();
    let mut channel2 = client.channel_open(4u16).await.unwrap();
    let mut consumer2 = channel2
        .basic_consume("consumer-view-2", NoAck(false), Exclusive(false), NoLocal(false))
        .await
        .unwrap();

    channel1
        .basic_publish("event-hub", "dont-care", PublishedMessage::default().text("Event #1"))
        .await
        .unwrap();

    let signal1 = consumer1.receive(Duration::from_secs(1)).await.unwrap();
    assert!(matches!(signal1, ConsumerSignal::Delivered(_)));

    if let ConsumerSignal::Delivered(msg) = signal1 {
        assert_eq!(msg.consumer_tag, consumer1.consumer_tag);
    }

    let signal2 = consumer2.receive(Duration::from_secs(1)).await.unwrap();
    assert!(matches!(signal2, ConsumerSignal::Delivered(_)));

    if let ConsumerSignal::Delivered(msg) = signal2 {
        assert_eq!(msg.consumer_tag, consumer2.consumer_tag);
    }

    consumer1.basic_cancel().await.unwrap();
    consumer2.basic_cancel().await.unwrap();
    channel1.close().await.unwrap();
    channel2.close().await.unwrap();
}
