use anyhow::Result;
use metalmq_client::Client;

#[allow(dead_code)]
pub async fn connect() -> Result<Client> {
    let c = metalmq_client::connect("localhost:5672", "guest", "guest").await?;
    c.open("/").await?;

    Ok(c)
}
