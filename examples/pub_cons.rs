use anyhow::Result;
use metalmq_client::*;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Barrier;

#[tokio::main]
async fn main() -> Result<()> {
    //console_subscriber::init();

    let exchange = "x_pubsub";
    let queue = "q_pubsub";

    let (mut client, _) = Client::connect("::1:5672", "guest", "guest").await?;

    let mut publisher = client.channel_open(1).await?;

    publisher
        .exchange_declare(exchange, ExchangeType::Direct, ExchangeDeclareOpts::default())
        .await?;
    publisher.queue_declare(queue, QueueDeclareOpts::default()).await?;
    publisher
        .queue_bind(queue, exchange, Binding::Direct("".to_string()))
        .await?;

    let mut consumer = client.channel_open(2).await?;

    let message_count = 1024u32;

    let barrier = Arc::new(Barrier::new(2));
    let consuming_finished = Arc::clone(&barrier);

    let mut handler = consumer
        .basic_consume(queue, NoAck(false), Exclusive(false), NoLocal(false))
        .await?;
    let mut received_count = 0u32;

    tokio::spawn(async move {
        while let Some(signal) = handler.signal_stream.recv().await {
            match signal {
                ConsumerSignal::Delivered(m) => {
                    received_count += 1;

                    handler.basic_ack(m.delivery_tag).await.unwrap();

                    if received_count >= message_count {
                        break;
                    }
                }
                ConsumerSignal::Cancelled
                | ConsumerSignal::ChannelClosed { .. }
                | ConsumerSignal::ConnectionClosed { .. } => {}
            }
        }

        handler.basic_cancel().await.unwrap();

        consuming_finished.wait().await;
    });

    let start = Instant::now();

    for _ in 0..message_count {
        let message =
            PublishedMessage::default().text("This will be the test message what we send over multiple times");

        publisher.basic_publish(exchange, "", message).await?;
    }

    barrier.wait().await;

    println!(
        "Send and receive {} messages: {:?}",
        message_count,
        Instant::elapsed(&start)
    );

    publisher.queue_unbind(queue, exchange, "").await.unwrap();
    publisher
        .queue_delete(queue, IfUnused(false), IfEmpty(false))
        .await
        .unwrap();
    publisher.exchange_delete(exchange, IfUnused(false)).await.unwrap();
    publisher.close().await?;
    consumer.close().await?;
    client.close().await?;

    Ok(())
}
