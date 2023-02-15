use anyhow::Result;
use metalmq_client::{Client, ClientError, EventHandler};

#[allow(dead_code)]
pub async fn connect() -> Result<(Client, EventHandler)> {
    Client::connect("localhost:5672", "guest", "guest").await
}

#[allow(dead_code)]
pub(crate) fn to_client_error<T: std::fmt::Debug>(result: Result<T>) -> ClientError {
    result.unwrap_err().downcast::<ClientError>().unwrap()
}
