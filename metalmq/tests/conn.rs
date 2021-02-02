extern crate metalmq_client;

mod helper {
    pub mod conn;
}

use crate::metalmq_client as client;

#[cfg(feature = "integration-tests")]
#[tokio::test]
async fn connect() -> client::Result<()> {
    let c = client::connect("127.0.0.1:5672").await?;

    Ok(())
}

#[cfg(feature = "integration-tests")]
#[tokio::test]
async fn open_already_opened_channel() -> client::Result<()> {
    let c = client::connect("127.0.0.1:5672").await?;
    c.open("/").await?;

    c.channel_open(1).await?;

    let result = c.channel_open(1).await;

    assert!(result.is_err());

    let err = metalmq_client::bdd::to_client_error(result);

    assert_eq!(err.code, 504);

    Ok(())
}
