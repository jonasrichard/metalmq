use anyhow::Result;
use metalmq_client::*;
use std::collections::HashMap;
use tokio::sync::oneshot;

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

        let client = Client::connect("localhost:5672", username, password).await?;

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
pub(crate) async fn declare_exchange_queue(ch: &Channel, exchange: &str, queue: &str) -> Result<()> {
    ch.exchange_declare(exchange, ExchangeType::Direct, ExchangeDeclareOpts::default())
        .await?;
    ch.queue_declare(queue, QueueDeclareOpts::default()).await?;

    ch.queue_bind(queue, exchange, Binding::Direct("".to_string())).await?;

    Ok(())
}

/// Make a new connection and on the 1st channel it deletes the exchange.
#[allow(dead_code)]
pub(crate) async fn delete_exchange(exchange: &str) -> Result<()> {
    let mut c = default().connect().await?;
    let ch = c.channel_open(1).await?;

    ch.exchange_delete(exchange, IfUnused(false)).await?;

    ch.close().await?;
    c.close().await?;

    Ok(())
}

/// Make a new connection and on the 1st channel it deletes the exchange.
#[allow(dead_code)]
pub(crate) async fn delete_queue(queue: &str) -> Result<()> {
    let mut c = default().connect().await?;
    let ch = c.channel_open(1).await?;

    ch.queue_delete(queue, IfUnused(false), IfEmpty(false)).await?;

    ch.close().await?;
    c.close().await?;

    Ok(())
}

/// Consumes `n` number of messages, collecting them and send back on the
/// `tx` sender as a vector of messages.
#[allow(dead_code)]
pub(crate) async fn consume_messages<'a>(
    client_channel: &'a Channel,
    queue: &'a str,
    exclusive: Exclusive,
    n: usize,
) -> Result<oneshot::Receiver<Vec<Message>>> {
    use tokio::time;

    let (tx, rx) = oneshot::channel();
    let mut handler = client_channel
        .basic_consume(queue, NoAck(false), exclusive, NoLocal(false))
        .await?;

    tokio::spawn(async move {
        let _ = &handler;
        let mut messages = vec![];

        loop {
            match time::timeout(time::Duration::from_secs(5), handler.signal_stream.recv()).await {
                Ok(Some(ConsumerSignal::Delivered(message))) => {
                    messages.push(message);

                    if messages.len() >= n {
                        break;
                    }
                }
                Ok(unexpected) => {
                    panic!("Unexpected signal {:?}", unexpected);
                }
                Err(e) => {
                    panic!("Error {:?}", e);
                }
            }
        }

        tx.send(messages).unwrap();
    });

    Ok(rx)
}
