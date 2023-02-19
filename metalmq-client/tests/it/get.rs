use std::time::Duration;

use metalmq_client::*;

use crate::helper;

#[tokio::test]
async fn test_get_logic() {
    let (mut client, mut handler) = helper::connect().await.unwrap();
    let mut channel = client.channel_open(1).await.unwrap();

    channel
        .exchange_declare("x-get", ExchangeType::Direct, ExchangeDeclareOpts::default())
        .await
        .unwrap();
    channel
        .queue_declare("q-get", QueueDeclareOpts::default())
        .await
        .unwrap();
    channel
        .queue_bind("q-get", "x-get", Binding::Direct("key".to_string()))
        .await
        .unwrap();

    get_with_ack(&mut client, &mut channel).await;

    channel.queue_unbind("q-get", "x-get", "key").await.unwrap();
    channel
        .queue_delete("q-get", IfUnused(false), IfEmpty(false))
        .await
        .unwrap();
    channel.exchange_delete("x-get", IfUnused(false)).await.unwrap();

    channel.close().await.unwrap();
    client.close().await.unwrap();
}

async fn get_with_ack(client: &mut Client, channel: &mut Channel) {
    let mut get_channel = client.channel_open(2).await.unwrap();

    channel
        .basic_publish("x-get", "key", PublishedMessage::default().text("Get #1"))
        .await
        .unwrap();

    let mut handler = get_channel.basic_get("q-get", NoAck(false)).await.unwrap();

    let get_signal = dbg!(handler.receive(Duration::from_secs(1)).await.unwrap());
    assert!(matches!(get_signal, GetSignal::GetOk { .. }));

    if let GetSignal::GetOk(gm) = get_signal {
        assert!(!gm.redelivered);
        assert_eq!(gm.exchange, "x-get");
        assert_eq!(gm.routing_key, "key");
        assert_eq!(gm.message.body, "Get #1".as_bytes().to_vec());

        handler.basic_ack(gm.delivery_tag).await.unwrap();
    }

    handler.close().await.unwrap();
    get_channel.close().await.unwrap();
}
