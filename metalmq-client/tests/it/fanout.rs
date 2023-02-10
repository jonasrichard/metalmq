use std::time::Duration;

use crate::{helper, message_from_string};
use metalmq_client::*;

#[tokio::test]
async fn test_routing_logic() {
    let mut client = helper::connect().await.unwrap();
    let channel = client.channel_open(2u16).await.unwrap();

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
}

async fn publish_deliver_message_to_all_queues(client: &mut Client) {
    let channel1 = client.channel_open(3u16).await.unwrap();
    let mut consumer1 = channel1
        .basic_consume("consumer-view-1", NoAck(false), Exclusive(false), NoLocal(false))
        .await
        .unwrap();
    let channel2 = client.channel_open(4u16).await.unwrap();
    let mut consumer2 = channel2
        .basic_consume("consumer-view-2", NoAck(false), Exclusive(false), NoLocal(false))
        .await
        .unwrap();

    channel1
        .basic_publish(
            "event-hub",
            "dont-care",
            message_from_string(channel1.channel, "Event #1".to_string()),
            Mandatory(false),
            Immediate(false),
        )
        .await
        .unwrap();

    let signal1 = consumer1.receive(Duration::from_secs(1)).await.unwrap();
    assert!(matches!(signal1, ConsumerSignal::Delivered(_)));

    if let ConsumerSignal::Delivered(msg) = signal1 {
        assert_eq!(msg.delivery_info.unwrap().consumer_tag, consumer1.consumer_tag);
    }

    let signal2 = consumer2.receive(Duration::from_secs(1)).await.unwrap();
    assert!(matches!(signal2, ConsumerSignal::Delivered(_)));

    if let ConsumerSignal::Delivered(msg) = signal2 {
        assert_eq!(msg.delivery_info.unwrap().consumer_tag, consumer2.consumer_tag);
    }

    consumer1.basic_cancel().await.unwrap();
    consumer2.basic_cancel().await.unwrap();
    channel1.close().await.unwrap();
    channel2.close().await.unwrap();
}
