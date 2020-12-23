extern crate ironmq_client;

mod helper {
    pub mod conn;
}

use crate::ironmq_client as client;
use helper::conn::default_connection;

#[cfg(feature = "integration-tests")]
#[tokio::test]
async fn consume() -> client::Result<()> {
    let exchange = "to-be-deleted";
    let queue = "queue-del";
    let c = default_connection(exchange, queue).await?;

    c.basic_consume(1, queue, "ctag", |msg| {
        println!("Message {:?}", msg);
        "".to_string()
    }).await?;

    c.basic_publish(1, exchange, "", "Hello".into()).await?;

    c.close().await?;

    Ok(())
}
