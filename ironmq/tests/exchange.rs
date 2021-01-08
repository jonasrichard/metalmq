extern crate ironmq_client;

use crate::ironmq_client as client;

#[cfg(feature = "integration-tests")]
#[tokio::test]
async fn channel_close_on_not_existing_exchange() -> client::Result<()> {
    let c = client::connect("127.0.0.1:5672").await?;

    let mut flags = ironmq_codec::frame::ExchangeDeclareFlags::empty();
    flags |= ironmq_codec::frame::ExchangeDeclareFlags::PASSIVE;

    let result = c.exchange_declare(1, "sure do not exist", "fanout", Some(flags)).await;

    assert!(result.is_err());

    let err = result.unwrap_err().downcast::<client::ClientError>().unwrap();
    assert_eq!(err.code, 404);

    Ok(())
}

#[cfg(feature = "integration-tests")]
#[tokio::test]
async fn passive_exchange_declare_check_if_exchange_exist() -> client::Result<()> {
    let c = client::connect("127.0.0.1:5672").await?;

    let mut flags = ironmq_codec::frame::ExchangeDeclareFlags::empty();
    c.exchange_declare(1, "new channel", "fanout", Some(flags)).await?;

    flags |= ironmq_codec::frame::ExchangeDeclareFlags::PASSIVE;
    let result = c.exchange_declare(1, "new channel", "fanout", Some(flags)).await;

    assert!(result.is_ok());

    Ok(())
}
