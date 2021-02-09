use cucumber::async_trait;
use std::convert::Infallible;

pub struct MyWorld {
    client: metalmq_client::Client,
    last_result: metalmq_client::Result<()>
}

impl MyWorld {
}

#[async_trait(?Send)]
impl cucumber::World for MyWorld {
    type Error = Infallible;

    async fn new() -> Result<Self, Infallible> {
        Ok(Self {
            client: metalmq_client::connect("127.0.0.1:5672").await.unwrap(),
            last_result: Ok(())
        })
    }
}

mod steps {
    use cucumber::{Steps, t};

    pub fn steps() -> Steps<super::MyWorld> {
        let mut builder: Steps<super::MyWorld> = Steps::new();

        builder
            .given_async("a client", t!(|mut world, step| {
                world.client.open("/").await.unwrap();
                world.client.channel_open(1).await.unwrap();
                world
            }))
            .when_async("declare an exchange test", t!(|mut world, step| {
                world.last_result = world.client.exchange_declare(1, "test", "fanout", None).await;
                world
            }))
            .then_async("it succeeds", t!(|mut world, step| {
                assert!(world.last_result.is_ok());
                world
            }));

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
