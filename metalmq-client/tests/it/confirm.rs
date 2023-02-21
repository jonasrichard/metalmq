use std::time::Duration;

use crate::helper;

use metalmq_client::*;

#[tokio::test]
async fn test_confirm_logic() {
    let (mut client, mut handler) = helper::connect().await.unwrap();
    let mut channel = client.channel_open_next().await.unwrap();

    channel
        .exchange_declare("safe-exchange", ExchangeType::Direct, ExchangeDeclareOpts::default())
        .await
        .unwrap();
    channel
        .queue_declare("safe-queue", QueueDeclareOpts::default())
        .await
        .unwrap();
    channel
        .queue_bind("safe-queue", "safe-exchange", Binding::Direct("".into()))
        .await
        .unwrap();

    publish_with_confirm_acks(&mut client, &mut handler).await;

    channel.queue_unbind("safe-queue", "safe-exchange", "").await.unwrap();
    channel
        .queue_delete("safe-queue", IfUnused(false), IfEmpty(false))
        .await
        .unwrap();
    channel.exchange_delete("safe-exchange", IfUnused(false)).await.unwrap();

    channel.close().await.unwrap();
    client.close().await.unwrap();
}

async fn publish_with_confirm_acks(client: &mut Client, handler: &mut EventHandler) {
    let mut confirmed = client.channel_open_next().await.unwrap();

    confirmed.confirm().await.unwrap();

    confirmed
        .basic_publish(
            "safe-exchange",
            "",
            PublishedMessage::default()
                .text("Confirmed message")
                .channel(confirmed.channel),
        )
        .await
        .unwrap();

    let ack = handler.receive_event(Duration::from_secs(1)).await.unwrap();
    assert!(matches!(ack, EventSignal::BasicAck { .. }));

    confirmed.close().await.unwrap();
}
