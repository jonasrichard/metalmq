use std::time::Duration;

use crate::{helper, message_from_string};
use metalmq_client::*;

#[tokio::test]
async fn test_routing_logic() {
    let (mut client, mut handler) = helper::connect().await.unwrap();
    let mut channel = client.channel_open(1u16).await.unwrap();

    channel
        .exchange_declare("images", ExchangeType::Direct, ExchangeDeclareOpts::default())
        .await
        .unwrap();
    channel
        .queue_declare("images", QueueDeclareOpts::default())
        .await
        .unwrap();
    channel
        .queue_bind("images", "images", Binding::Direct("images".to_string()))
        .await
        .unwrap();

    channel.queue_purge("images").await.unwrap();

    basic_publish_mandatory_delivered(&channel).await;
    basic_publish_mandatory_unrouted_return(&mut handler, &channel).await;

    channel.confirm().await.unwrap();

    publish_confirm_mode_sends_ack(&mut handler, &channel).await;

    channel.queue_unbind("images", "images", "images").await.unwrap();
    channel
        .queue_delete("images", IfUnused(false), IfEmpty(false))
        .await
        .unwrap();
    channel.exchange_delete("images", IfUnused(false)).await.unwrap();

    channel.close().await.unwrap();
    client.close().await.unwrap();
}

async fn basic_publish_mandatory_delivered(channel: &Channel) {
    let mut handler = channel
        .basic_consume("images", NoAck(false), Exclusive(false), NoLocal(false))
        .await
        .unwrap();

    channel
        .basic_publish(
            "images",
            "images",
            message_from_string(channel.channel, "An image").mandatory(true),
        )
        .await
        .unwrap();

    let signal = handler.receive(Duration::from_secs(1)).await.unwrap();

    if let ConsumerSignal::Delivered(message) = signal {
        handler.basic_ack(message.delivery_tag).await.unwrap();
    }

    handler.basic_cancel().await.unwrap();
}

async fn basic_publish_mandatory_unrouted_return(handler: &mut EventHandler, channel: &Channel) {
    channel
        .basic_publish(
            "images",
            "extension.txt",
            message_from_string(channel.channel, "A text file").mandatory(true),
        )
        .await
        .unwrap();

    let evt = handler.receive_event(Duration::from_secs(1)).await.unwrap();

    assert!(matches!(evt, EventSignal::BasicReturn { .. }));
}

async fn publish_confirm_mode_sends_ack(handler: &mut EventHandler, channel: &Channel) {
    channel
        .basic_publish("images", "images", message_from_string(channel.channel, "A text file"))
        .await
        .unwrap();

    let evt = handler.receive_event(Duration::from_secs(1)).await.unwrap();

    assert!(matches!(evt, EventSignal::BasicAck { .. }));
}
