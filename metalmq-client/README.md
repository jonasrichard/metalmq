# MetalMQ client

MetalMQ client is an AMQP 0.9.1 compatible client implemented fully in Rust. It uses the
metalmq-codec crate to parse binary packets as AMQP frames. Also it is tied to tokio async
runtime.

See [docs.rs](https://docs.rs/metalmq-client) for the library documentation.

## Usage

Add dependency to `Cargo.toml`

```toml
[dependencies]
metalmq-client = "0.3"
```

Start with a simple `main.rs`

```rust
use metalmq_client::*;

#[tokio::main]
async fn main() {
    let mut client = Client::connect("localhost:5672", "guest", "guest").await.unwrap();
    let channel = client.channel_open(1).await.unwrap();

    let mut handler = consumer.basic_consume("queue", NoAck(false), Exclusive(false),
        NoLocal(false)).await.unwrap();

    match handler.signal_stream.recv().await {
        Ok(ConsumerSignal::Delivered(delivered_message)) => {
            channel.basic_ack(delivered_message.delivery_tag).await.unwrap();
        }
        Ok(_) => (),
        Err(_) => (),
    }
}
```
