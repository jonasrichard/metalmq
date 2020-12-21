extern crate ironmq_client;

use crate::ironmq_client as client;

#[cfg(feature = "integration-tests")]
#[tokio::test]
async fn consume() -> client::Result<()> {
    let exchange = "to-be-deleted";
    let queue = "queue-del";

    let c = client::connect("127.0.0.1:5672").await?;
    c.exchange_declare(1, exchange, "fanout", None).await?;
    c.queue_declare(1, queue).await?;
    c.queue_bind(1, queue, exchange, "").await?;

    c.close().await?;

    Ok(())
}
