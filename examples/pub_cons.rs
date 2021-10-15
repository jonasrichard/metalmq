use anyhow::Result;
use log::{error, info};
use metalmq_client::*;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot};

struct Counter {
    counter: u32,
}

impl Consumer for Counter {
    fn on_message(&mut self, message: Message) -> ConsumeResult {
        self.counter += 1;

        let cancel = self.counter == 1024;

        ConsumeResult::Ack {
            delivery_tag: message.delivery_tag,
            multiple: false,
            cancel,
        }
    }
}

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

    let message_count = 1024u32;

    let counter = Counter { counter: 0 };

    let consume_result = channel.basic_consume(queue, "ctag", None, Box::new(counter)).await?;

    let message = "This will be the test message what we send over multiple times";

    let start = Instant::now();

    for _ in 0..message_count {
        channel.basic_publish(exchange, "", message.to_string()).await?;
    }

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
