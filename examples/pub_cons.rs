use anyhow::Result;
use log::info;
use metalmq_client::*;
use rand::prelude::*;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<()> {
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

    let message_count = 1u32;
    let mut received_count = 0u32;

    let counter = move |i: ConsumerSignal| match i {
        ConsumerSignal::Delivered(m) => {
            received_count += 1;

            ConsumerResponse {
                result: None,
                ack: ConsumerAck::Ack {
                    delivery_tag: m.delivery_tag,
                    multiple: false,
                },
            }
        }
        ConsumerSignal::Cancelled | ConsumerSignal::ChannelClosed | ConsumerSignal::ConnectionClosed => {
            info!("Consuming cancelled after {} messages received", received_count);

            ConsumerResponse {
                result: Some(received_count),
                ack: ConsumerAck::Nothing,
            }
        }
    };

    let consume_result = consumer.basic_consume(queue, "ctag", None, Box::new(counter)).await?;

    let message = "This will be the test message what we send over multiple times";

    let start = Instant::now();

    for _ in 0..message_count {
        publisher.basic_publish(exchange, "", message.to_string()).await?;
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }

    let ctag_num: u32 = rng.gen();
    consumer.basic_cancel(&format!("ctag-{}", ctag_num)).await?;

    let received_messages = consume_result.await.unwrap();

    println!(
        "Received {} messages from the {}",
        received_messages.unwrap(),
        message_count
    );

    println!(
        "Send and receive {} messages: {:?}",
        message_count,
        Instant::elapsed(&start)
    );

    publisher.close().await?;
    consumer.close().await?;
    client.close().await?;

    Ok(())
}
