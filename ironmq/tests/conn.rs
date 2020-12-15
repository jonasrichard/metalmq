extern crate ironmq_client;

use crate::ironmq_client as client;

#[cfg(feature = "integration-tests")]
#[tokio::test]
async fn connect() -> client::Result<()> {
    let c = client::connect("127.0.0.1:5672".to_string()).await?;

    Ok(())
}
