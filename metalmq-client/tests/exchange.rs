use anyhow::Result;
use cucumber::async_trait;
use std::convert::Infallible;

pub struct MyWorld {
    sender: metalmq_client::Client,
    receiver: metalmq_client::Client,
    last_result: Result<()>,
}

impl MyWorld {}

#[async_trait(?Send)]
impl cucumber::World for MyWorld {
    type Error = Infallible;

    async fn new() -> Result<Self, Infallible> {
        Ok(Self {
            sender: metalmq_client::connect("127.0.0.1:5672", "guest", "guest")
                .await
                .unwrap(),
            receiver: metalmq_client::connect("127.0.0.1:5672", "guest", "guest")
                .await
                .unwrap(),
            last_result: Ok(()),
        })
    }
}

mod steps {
    use cucumber::{t, Steps};

    pub fn steps() -> Steps<super::MyWorld> {
        let mut builder: Steps<super::MyWorld> = Steps::new();

        builder.given_regex_async(
            "an exchange declared as (.*)",
            t!(|mut world, _matches, _step| {
                world.sender.open("/").await.unwrap();
                world.sender.channel_open(1).await.unwrap();
                world
                    .sender
                    .exchange_declare(1, "message-exchange", "topic", None)
                    .await
                    .unwrap();

                world
            }),
        );

        builder
    }
}

async fn main() {
    metalmq_client::setup_logger();

    cucumber::Cucumber::<MyWorld>::new()
        .features(&["./features"])
        .steps(steps::steps())
        .cli()
        .run_and_exit()
        .await
}
