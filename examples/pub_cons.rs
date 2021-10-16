use anyhow::Result;
use log::{error, info};
use metalmq_client::*;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot};

#[tokio::main]
async fn main() -> Result<()> {
    let exchange = "x_pubsub";
    let queue = "q_pubsub";

    metalmq_client::setup_logger();

    let mut client = metalmq_client::connect("localhost:5672", "guest", "guest").await?;
    client.open("/").await?;

    let channel = client.channel_open(1).await?;

    channel.exchange_declare(exchange, "direct", None).await?;
    channel.queue_declare(queue, None).await?;
    channel.queue_bind(queue, exchange, "").await?;

    let message_count = 128u32;
    let mut received_count = 0u32;

    let counter = move |i: ConsumeInput| {
        match i {
            ConsumeInput::Delivered(m) => {
                received_count += 1;

                ConsumeResult {
                    result: None,
                    ack_response: ConsumeResponse::Ack {
                        delivery_tag: m.delivery_tag,
                        multiple: false,
                    }
                }
            },
            ConsumeInput::Cancelled | ConsumeInput::Error =>
                ConsumeResult {
                    result: Some(received_count),
                    ack_response: ConsumeResponse::Nothing,
                },
        }
    };

    let consume_result = channel.basic_consume(queue, "ctag", None, Box::new(counter)).await?;

    let message = "This will be the test message what we send over multiple times";

    let start = Instant::now();

    for _ in 0..message_count {
        channel.basic_publish(exchange, "", message.to_string()).await?;
    }

    channel.basic_cancel("ctag").await?;

    consume_result.await.unwrap();

    println!(
        "Send and receive {} messages: {:?}",
        message_count,
        Instant::elapsed(&start)
    );

    channel.close().await?;
    client.close().await?;

    Ok(())
}
