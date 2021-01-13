extern crate ironmq_client;

mod helper {
    pub mod conn;
}

use crate::ironmq_client as client;

#[cfg(feature = "integration-tests")]
#[tokio::test]
async fn publish_to_non_existing_exchange() -> client::Result<()> {
    let c = client::connect("127.0.0.1:5672").await?;
    c.open("/").await?;
    c.channel_open(1).await?;

    let result = c.basic_publish(1, "non existent", "any key", "data".to_string()).await;

    assert!(result.is_err());

    Ok(())
}

#[cfg(feature = "integration-tests")]
#[tokio::test]
async fn publish_to_default_exchange() -> client::Result<()> {
    let c = client::connect("127.0.0.1:5672").await?;
    c.open("/").await?;
    c.channel_open(1).await?;

    // TODO connect default exchange to a queue to see the message published there

    let result = c.basic_publish(1, "", "any key", "data".to_string()).await;

    assert!(result.is_ok());

    Ok(())
}

#[cfg(feature = "integration-tests")]
#[tokio::test]
async fn publish_to_intenal_exchange() -> client::Result<()> {
    let c = client::connect("127.0.0.1:5672").await?;
    c.open("/").await?;
    c.channel_open(1).await?;

    let mut flags = ironmq_codec::frame::ExchangeDeclareFlags::empty();
    flags |= ironmq_codec::frame::ExchangeDeclareFlags::INTERNAL;

    c.exchange_declare(1, "internal", "fanout", Some(flags)).await?;
    c.queue_bind(1, "normal-queue", "internal", "").await?;

    let result = c.basic_publish(1, "internal", "any key", "data".to_string()).await;

    assert!(result.is_err());

    let err = helper::conn::to_client_error(result);
    assert_eq!(err.code, 403);

    Ok(())
}

// TODO test if exchange refuses basic content (how and why it does?)
