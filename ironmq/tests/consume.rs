extern crate ironmq_client;

mod helper {
    pub mod conn;
}

use crate::ironmq_client as client;
use helper::conn::default_connection;
use tokio::sync::{mpsc, oneshot};

#[cfg(feature = "integration-tests")]
#[tokio::test]
async fn consume() -> client::Result<()> {
    let exchange = "to-be-deleted";
    let queue = "queue-del";
    let c = default_connection(exchange, queue).await?;

    let (otx, orx) = oneshot::channel();

    let (tx, mut rx) = mpsc::channel(1);
    tokio::spawn(async move {
        let mut count = 0;

        while let Some(msg) = rx.recv().await {
            count += 1;
            if count == 1 {
                break
            }
        }
        otx.send(()).unwrap();
    });

    c.basic_consume(1, queue, "ctag", tx).await?;
    c.basic_publish(1, exchange, "", "Hello".into()).await?;

    orx.await.unwrap();

    c.channel_close(1).await?;
    c.close().await?;

    Ok(())
}
