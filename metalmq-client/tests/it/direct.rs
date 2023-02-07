use std::time::Duration;

use crate::helper;
use metalmq_client::*;

#[tokio::test]
async fn test_routing_logic() {
    let mut client = helper::connect().await.unwrap();
    let channel = client.channel_open(1u16).await.unwrap();

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
    basic_publish_mandatory_unrouted_return(&mut client, &channel).await;

    channel.confirm().await.unwrap();

    publish_confirm_mode_sends_ack(&mut client, &channel).await;

    channel.close().await.unwrap();
    client.close().await.unwrap();
}

async fn basic_publish_mandatory_delivered(channel: &Channel) {
    let mut handler = channel
        .basic_consume("images", "ctag1", NoAck(false), Exclusive(false), NoLocal(false))
        .await
        .unwrap();

    channel
        .basic_publish(
            "images",
            "images",
            "An image".to_string(),
            Mandatory(true),
            Immediate(false),
        )
        .await
        .unwrap();

    let signal = handler.receive(Duration::from_secs(1)).await.unwrap();

    if let ConsumerSignal::Delivered(message) = signal {
        handler.basic_ack(message.delivery_tag).await.unwrap();
    }

    handler.basic_cancel().await.unwrap();
}

async fn basic_publish_mandatory_unrouted_return(client: &mut Client, channel: &Channel) {
    channel
        .basic_publish(
            "images",
            "extension.txt",
            "A text file".to_string(),
            Mandatory(true),
            Immediate(false),
        )
        .await
        .unwrap();

    let evt = client.receive_event(Duration::from_secs(1)).await.unwrap();

    assert!(matches!(evt, EventSignal::BasicReturn { .. }));
}

async fn publish_confirm_mode_sends_ack(client: &mut Client, channel: &Channel) {
    channel
        .basic_publish(
            "images",
            "images",
            "A text file".to_string(),
            Mandatory(false),
            Immediate(false),
        )
        .await
        .unwrap();

    let evt = client.receive_event(Duration::from_secs(1)).await.unwrap();

    assert!(matches!(evt, EventSignal::BasicAck { .. }));
}
