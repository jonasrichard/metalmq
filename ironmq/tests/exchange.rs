extern crate ironmq_client;

mod helper {
    pub mod conn;
}

use crate::ironmq_client as client;
use std::io::Write;
use termcolor::{Color, ColorChoice, ColorSpec, StandardStream, WriteColor};

fn write(color: Color, text: &str) {
    let mut stdout = StandardStream::stdout(ColorChoice::Always);
    stdout.set_color(ColorSpec::new().set_fg(Some(color)));

    writeln!(&mut stdout, "{}", text).unwrap();

    stdout.reset();
}

struct World {
    client: Box<dyn client::Client>
}

impl World {
    async fn given_a_connection() -> Self {
        write(Color::Green, "Given a connection");

        let client = client::connect("127.0.0.1:5672").await.unwrap();

        World {
            client: client
        }
    }

    async fn when_it_opens_the_virtual_host(&self) -> &Self {
        write(Color::Yellow, "When it opens the virtual host");

        self.client.open("/").await.unwrap();

        self
    }

    async fn then_it_succeeds(&self) {
        println!("Then is succeeds");
    }
}

#[tokio::test]
async fn first() {
    World
        ::given_a_connection().await
        .when_it_opens_the_virtual_host().await
        .then_it_succeeds().await;
}

#[cfg(feature = "integration-tests")]
#[tokio::test]
async fn channel_close_on_not_existing_exchange() -> client::Result<()> {
    let c = client::connect("127.0.0.1:5672").await?;

    let mut flags = ironmq_codec::frame::ExchangeDeclareFlags::empty();
    flags |= ironmq_codec::frame::ExchangeDeclareFlags::PASSIVE;

    let result = c.exchange_declare(1, "sure do not exist", "fanout", Some(flags)).await;

    assert!(result.is_err());

    let err = helper::conn::to_client_error(result);
    assert_eq!(err.code, 404);

    Ok(())
}

#[cfg(feature = "integration-tests")]
#[tokio::test]
async fn passive_exchange_declare_check_if_exchange_exist() -> client::Result<()> {
    let c = client::connect("127.0.0.1:5672").await?;

    let mut flags = ironmq_codec::frame::ExchangeDeclareFlags::empty();
    c.exchange_declare(1, "new channel", "fanout", Some(flags)).await?;

    flags |= ironmq_codec::frame::ExchangeDeclareFlags::PASSIVE;
    let result = c.exchange_declare(1, "new channel", "fanout", Some(flags)).await;

    assert!(result.is_ok());

    Ok(())
}
