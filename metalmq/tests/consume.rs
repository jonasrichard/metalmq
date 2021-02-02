extern crate metalmq_client;

mod helper {
    pub mod conn;
}

use crate::metalmq_client as client;
use helper::conn::default_connection;
use tokio::sync::{oneshot};

#[cfg(feature = "integration-tests")]
#[tokio::test]
async fn consume() -> client::Result<()> {
    let exchange = "to-be-deleted";
    let queue = "queue-del";
    let c = default_connection(exchange, queue).await?;

    let (otx, orx) = oneshot::channel();
    helper::conn::consume_messages(&c, 1, queue, "ctag", otx, 1).await?;

    c.basic_publish(1, exchange, "", "Hello".into()).await?;

    let msgs = orx.await.unwrap();
    assert_eq!(msgs.len(), 1);

    c.channel_close(1).await?;
    c.close().await?;

    Ok(())
}
