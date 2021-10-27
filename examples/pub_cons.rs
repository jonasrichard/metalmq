use anyhow::Result;
use log::{debug, info};
use metalmq_client::*;
use rand::prelude::*;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::sync::Barrier;

#[tokio::main]
async fn main() -> Result<()> {
    //console_subscriber::init();

    let mut rng = rand::thread_rng();

    let exchange = "x_pubsub";
    let queue = "q_pubsub";

    metalmq_client::setup_logger();

    let mut client = metalmq_client::connect("localhost:5672", "guest", "guest").await?;

    let publisher = client.channel_open(1).await?;

    publisher.exchange_declare(exchange, "direct", None).await?;
    publisher.queue_declare(queue, None).await?;
    publisher.queue_bind(queue, exchange, "").await?;

    let consumer = client.channel_open(2).await?;

    let message_count = 1024u32;

    let barrier = Arc::new(Barrier::new(2));
    let consuming_finished = Arc::clone(&barrier);

    let ctag = format!("ctag-{}", rng.gen::<u32>());

    let mut handler = consumer.basic_consume(queue, &ctag, None).await?;
    let mut received_count = 0u32;

    tokio::spawn(async move {
        while let Some(signal) = handler.signal_stream.recv().await {
            match signal {
                ConsumerSignal::Delivered(m) => {
                    received_count += 1;

                    handler.basic_ack(m.delivery_tag).await;

                    if received_count >= message_count {
                        break;
                    }
                }
                ConsumerSignal::Cancelled | ConsumerSignal::ChannelClosed | ConsumerSignal::ConnectionClosed => {}
            }
        }

        handler.basic_cancel().await;

        consuming_finished.wait();
    });

    let message = "This will be the test message what we send over multiple times";

    let start = Instant::now();

    for _ in 0..message_count {
        publisher.basic_publish(exchange, "", message.to_string()).await?;
    }

    barrier.wait();

    info!(
        "Send and receive {} messages: {:?}",
        message_count,
        Instant::elapsed(&start)
    );

    publisher.close().await?;
    consumer.close().await?;
    client.close().await?;

    Ok(())
}
