use metalmq_client::{ExchangeDeclareOpts, ExchangeType};

use crate::helper;

#[tokio::test]
async fn delete_queue_when_last_consumer_left() {
    let mut declare = helper::default().connect().await.unwrap();
    let declare_channel = declare.channel_open(3u16).await.unwrap();

    declare_channel
        .exchange_declare(
            "exchange-for-auto-delete",
            ExchangeType::Direct,
            ExchangeDeclareOpts::default().auto_delete(true),
        )
        .await
        .unwrap();
}
