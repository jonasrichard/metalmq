use anyhow::Result;
use metalmq_client::*;
use metalmq_codec::frame::{BasicConsumeFlags, ExchangeDeclareFlags};
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

/// The helper connection.
#[allow(dead_code)]
pub(crate) struct ConnData<'a> {
    params: HashMap<&'a str, &'a str>,
}

/// Create connection with the default data, virtual host is "/",
/// user and password are "guest".
#[allow(dead_code)]
pub(crate) fn default<'a>() -> ConnData<'a> {
    let mut p = HashMap::new();

    p.insert("username", "guest");
    p.insert("password", "guest");
    p.insert("virtual_host", "/");

    ConnData { params: p }
}

impl<'c, 'a: 'c> ConnData<'c> {
    /// Change username of the connection is being built.
    #[allow(dead_code)]
    pub(crate) fn with_username(&'c mut self, username: &'a str) -> &'c Self {
        self.params.insert("username", username);

        self
    }

    /// Change password of the connection is being built.
    #[allow(dead_code)]
    pub(crate) fn with_password(&'c mut self, password: &'a str) -> &'c Self {
        self.params.insert("password", password);

        self
    }

    /// Change virtual host of the connection is being built.
    #[allow(dead_code)]
    pub(crate) fn with_virtual_host(&'c mut self, virtual_host: &'a str) -> &'c Self {
        self.params.insert("virtual_host", virtual_host);

        self
    }

    /// Acutally connect to the server with the parameters we have set.
    #[allow(dead_code)]
    pub(crate) async fn connect(self) -> Result<Client> {
        let username = self.params.get("username").unwrap();
        let password = self.params.get("password").unwrap();
        let virtual_host = self.params.get("virtual_host").unwrap();

        let client = metalmq_client::connect("localhost:5672", username, password).await?;

        client.open(virtual_host).await?;

        Ok(client)
    }
}

/// Unwrap and downcast the error as a `ClientError`.
#[allow(dead_code)]
pub(crate) fn to_client_error<T: std::fmt::Debug>(result: Result<T>) -> ClientError {
    result.unwrap_err().downcast::<ClientError>().unwrap()
}

/// Declare an exchange and the queue and bind them. Exchange will be auto-delete.
#[allow(dead_code)]
pub(crate) async fn declare_exchange_queue(ch: &ClientChannel, exchange: &str, queue: &str) -> Result<()> {
    let mut ex_flags = ExchangeDeclareFlags::empty();
    ex_flags |= ExchangeDeclareFlags::AUTO_DELETE;

    ch.exchange_declare(exchange, "direct", Some(ex_flags)).await?;
    ch.queue_declare(queue, None).await?;

    ch.queue_bind(queue, exchange, "").await?;

    Ok(())
}

/// Make a new connection and on the 1st channel it deletes the exchange.
#[allow(dead_code)]
pub(crate) async fn delete_exchange(exchange: &str) -> Result<()> {
    let mut c = default().connect().await?;
    let ch = c.channel_open(1).await?;

    ch.exchange_delete(exchange, false).await?;

    Ok(())
}

/// Consumes `n` number of messages, collecting them and send back on the
/// `tx` sender as a vector of messages.
#[allow(dead_code)]
pub(crate) async fn consume_messages<'a>(
    client_channel: &'a ClientChannel,
    queue: &'a str,
    ctag: &'a str,
    flags: Option<BasicConsumeFlags>,
    n: usize,
) -> Result<oneshot::Receiver<Option<Vec<Message>>>> {
    use std::sync::{Arc, Mutex};

    let messages = Arc::new(Mutex::new(Vec::<Message>::new()));

    let receiver = move |input: ConsumeInput| match input {
        ConsumeInput::Delivered(message) => {
            let delivery_tag = message.delivery_tag;
            let mut msgs = messages.lock().unwrap();
            msgs.push(message);

            let response = ConsumeResponse::Ack {
                delivery_tag: delivery_tag,
                multiple: false,
            };

            if msgs.len() >= n {
                let ms = msgs.drain(0..).collect();

                ConsumeResult {
                    result: Some(ms),
                    ack_response: response,
                }
            } else {
                ConsumeResult {
                    result: None,
                    ack_response: response,
                }
            }
        }
        _ => ConsumeResult {
            result: None,
            ack_response: ConsumeResponse::Nothing,
        },
    };

    client_channel
        .basic_consume(queue, ctag, flags, Box::new(receiver))
        .await
}
