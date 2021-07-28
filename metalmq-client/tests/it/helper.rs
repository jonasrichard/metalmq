use anyhow::Result;
use metalmq_client::*;

#[allow(dead_code)]
pub async fn connect() -> Result<Client> {
    let c = metalmq_client::connect("localhost:5672", "guest", "guest").await?;
    c.open("/").await?;

    Ok(c)
}

#[allow(dead_code)]
pub(crate) fn to_client_error<T: std::fmt::Debug>(result: Result<T>) -> ClientError {
    result.unwrap_err().downcast::<ClientError>().unwrap()
}
