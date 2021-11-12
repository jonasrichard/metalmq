extern crate metalmq_client;

mod helper;

use anyhow::Result;
use metalmq_codec::frame::ExchangeDeclareFlags;

#[tokio::test]
async fn unrouted_mandatory_messages_gives_basic_return() -> Result<()> {
    let mut c = helper::default().connect().await?;
    let ch = c.channel_open(11).await?;

    let flags = ExchangeDeclareFlags::default();
    ch.exchange_declare("x-unrouted", "direct", Some(flags)).await?;

    ch.basic_publish("x-unrouter", "", "unrouted message".to_owned()).await;

    Ok(())
}
