use crate::helper;
use metalmq_client::*;

#[tokio::test]
async fn message_with_routing_key_as_exchange_name() {
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
    channel.queue_unbind("images", "images", "images").await.unwrap();

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

    let signal = handler.signal_stream.recv().await.unwrap();

    assert!(matches!(signal, ConsumerSignal::Delivered(_)));
}
