use anyhow::Result;
use cucumber::async_trait;
use std::convert::Infallible;

pub struct MyWorld {
    client: metalmq_client::Client,
    last_result: Result<()>,
}

impl MyWorld {
    fn take_err(&mut self) -> Option<metalmq_client::ClientError> {
        let mut result = Ok(());

        std::mem::swap(&mut self.last_result, &mut result);

        match result {
            Ok(()) => None,
            Err(e) => match e.downcast::<metalmq_client::ClientError>() {
                Ok(ce) => Some(ce),
                Err(_) => None,
            },
        }
    }
}

#[async_trait(?Send)]
impl cucumber::World for MyWorld {
    type Error = Infallible;

    async fn new() -> Result<Self, Infallible> {
        Ok(Self {
            client: metalmq_client::connect("localhost:5672", "guest", "guest")
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

        builder
            .given_async(
                "a client",
                t!(|mut world, _step| {
                    world.client.open("/").await.unwrap();
                    world.client.channel_open(1).await.unwrap();
                    world
                }),
            )
            .when_regex_async(
                "declare an exchange (.*)",
                t!(|mut world, matches, _step| {
                    world.last_result = world.client.exchange_declare(1, &matches[1], "fanout", None).await;
                    world
                }),
            )
            .then_async(
                "it succeeds",
                t!(|mut world, _step| {
                    assert!(world.last_result.is_ok());
                    world
                }),
            )
            .when_regex_async(
                "passive declare an exchange (.*)",
                t!(|mut world, matches, _step| {
                    let mut flags = metalmq_codec::frame::ExchangeDeclareFlags::empty();
                    flags |= metalmq_codec::frame::ExchangeDeclareFlags::PASSIVE;

                    world.last_result = world
                        .client
                        .exchange_declare(1, &matches[1], "fanout", Some(flags))
                        .await;
                    world
                }),
            )
            .then_regex_async(
                "it closes the channel with error code (.*)",
                t!(|mut world, matches, _step| {
                    let res = world.take_err();

                    assert!(res.is_some());

                    let err = res.unwrap();
                    assert_eq!(err.channel, Some(1));
                    assert_eq!(err.code, matches[1].parse::<u16>().unwrap());

                    world
                }),
            )
            .when_regex_async(
                "an exchange is declared with name (.*)",
                t!(|mut world, matches, _step| {
                    world.last_result = world.client.exchange_declare(1, &matches[1], "fanout", None).await;
                    world
                }),
            )
            .then_regex_async(
                "passively declare exchange (.*) succeeds",
                t!(|mut world, matches, _step| {
                    let mut flags = metalmq_codec::frame::ExchangeDeclareFlags::empty();
                    flags |= metalmq_codec::frame::ExchangeDeclareFlags::PASSIVE;

                    let result = world
                        .client
                        .exchange_declare(1, &matches[1], "fanout", Some(flags))
                        .await;
                    assert!(result.is_ok());

                    world
                }),
            );

        builder
    }
}

#[tokio::main]
async fn main() {
    cucumber::Cucumber::<MyWorld>::new()
        .features(&["./features"])
        .steps(steps::steps())
        .cli()
        .run_and_exit()
        .await
}
