use ironmq_client::*;
use ironmq_codec::frame::ExchangeDeclareFlags;

pub(crate) async fn default_connection(exchange: &str, queue: &str) -> Result<Box<dyn Client>> {
    let c = connect("127.0.0.1:5672").await?;
    c.open("/").await?;
    c.channel_open(1).await?;

    let mut ex_flags = ExchangeDeclareFlags::empty();
    ex_flags |= ExchangeDeclareFlags::AUTO_DELETE;
    c.exchange_declare(1, exchange, "fanout", Some(ex_flags)).await?;
    c.queue_declare(1, queue).await?;

    c.queue_bind(1, queue, exchange, "").await?;

    Ok(c)
}
