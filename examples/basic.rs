use ironmq_client as client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let exchange = "test-xchg";
    let queue = "test-queue";

    ironmq_client::setup_logger();

    let conn = ironmq_client::connect("127.0.0.1:5672".into()).await?;
    ironmq_client::open(&conn, "/".into()).await?;
    ironmq_client::channel_open(&conn, 1).await?;

    println!("Before");
    ironmq_client::exchange_declare(&conn, 1, exchange, "fanout", None).await?;
    println!("After");
    ironmq_client::queue_declare(&conn, 1, queue).await?;
    ironmq_client::queue_bind(&conn, 1, queue, exchange, "").await?;

    ironmq_client::basic_publish(&conn, 1, exchange, "no-key", "Hey man".into()).await?;

    ironmq_client::channel_close(&conn, 1).await?;
    ironmq_client::close(&conn).await?;

    Ok(())
}
