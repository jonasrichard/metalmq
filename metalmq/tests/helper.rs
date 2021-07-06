use anyhow::Result;
use metalmq_client::*;
use metalmq_codec::frame::ExchangeDeclareFlags;
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

#[allow(dead_code)]
pub(crate) struct ConnData<'a> {
    params: HashMap<&'a str, &'a str>,
}

#[allow(dead_code)]
pub(crate) fn default<'a>() -> ConnData<'a> {
    let mut p = HashMap::new();

    p.insert("username", "guest");
    p.insert("password", "guest");
    p.insert("virtual_host", "/");

    ConnData { params: p }
}

impl<'c, 'a: 'c> ConnData<'c> {
    #[allow(dead_code)]
    pub(crate) fn with_username(&'c mut self, username: &'a str) -> &'c Self {
        self.params.insert("username", username);

        self
    }

    #[allow(dead_code)]
    pub(crate) fn with_password(&'c mut self, password: &'a str) -> &'c Self {
        self.params.insert("password", password);

        self
    }

    #[allow(dead_code)]
    pub(crate) fn with_virtual_host(&'c mut self, virtual_host: &'a str) -> &'c Self {
        self.params.insert("virtual_host", virtual_host);

        self
    }

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

#[allow(dead_code)]
pub(crate) async fn declare_exchange_queue(ch: &ClientChannel, exchange: &str, queue: &str) -> Result<()> {
    let mut ex_flags = ExchangeDeclareFlags::empty();
    ex_flags |= ExchangeDeclareFlags::AUTO_DELETE;

    ch.exchange_declare(exchange, "direct", Some(ex_flags)).await?;
    ch.queue_declare(queue).await?;

    ch.queue_bind(queue, exchange, "").await?;

    Ok(())
}

#[allow(dead_code)]
pub(crate) fn to_client_error<T: std::fmt::Debug>(result: Result<T>) -> ClientError {
    result.unwrap_err().downcast::<ClientError>().unwrap()
}

#[allow(dead_code)]
pub(crate) async fn consume_messages<'a>(
    client_channel: &'a ClientChannel,
    queue: &'a str,
    ctag: &'a str,
    tx: oneshot::Sender<Vec<Message>>,
    n: usize,
) -> Result<()> {
    let (sink, mut source) = mpsc::channel(1);

    tokio::spawn(async move {
        let mut messages = vec![];

        while let Some(msg) = source.recv().await {
            messages.push(msg);

            if messages.len() == n {
                break;
            }
        }

        tx.send(messages).unwrap();
    });

    client_channel.basic_consume(queue, ctag, sink).await?;

    Ok(())
}
