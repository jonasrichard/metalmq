extern crate ironmq_client;

use crate::ironmq_client as client;

#[cfg(feature = "integration-tests")]
#[tokio::test]
async fn consume() -> client::Result<()> {
    let exchange = "to-be-deleted";
    let queue = "queue-del";

    let c = client::connect("127.0.0.1:5672".to_string()).await?;
    client::exchange_declare(&c, 1, exchange, "fanout", None).await?;
    client::queue_declare(&c, 1, queue).await?;
    client::queue_bind(&c, 1, queue, exchange, "").await?;

    client::close(&c).await?;

    Ok(())
}
