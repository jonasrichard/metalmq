use anyhow::Result;
use cucumber::async_trait;
use std::convert::Infallible;

pub struct MyWorld {
    client: Option<metalmq_client::Client>,
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
            client: None,
            last_result: Ok(()),
        })
    }
}

// TODO: info!(target: "test-logger", "msg", ...)
mod steps {
    use cucumber::{t, Steps};

    pub fn steps() -> Steps<super::MyWorld> {
        let mut builder: Steps<super::MyWorld> = Steps::new();

        builder
            .given_async("a user", t!(|mut world, _step| world))
            .when_regex_async(
                "connects as (.*)/(.*)",
                t!(|mut world, matches, _step| {
                    match metalmq_client::connect("127.0.0.1:5672", &matches[1], &matches[2]).await {
                        Ok(c) => world.client = Some(c),
                        Err(e) => world.last_result = Err(e),
                    }

                    world
                }),
            )
            .then_async(
                "it has been connected",
                t!(|mut world, _step| {
                    if world.client.is_none() {
                        log::error!("Error {:?}", world.last_result);
                    }
                    assert!(world.client.is_some());
                    world
                }),
            )
            .then_async(
                "it gets connection closed error",
                t!(|mut world, _step| {
                    let maybe_err = world.take_err();

                    assert!(maybe_err.is_some());

                    let err = maybe_err.unwrap();

                    assert_eq!(err.channel, None);
                    assert_eq!(err.code, 503);
                    assert_eq!(
                        err.message,
                        "Server closed connection during authentication".to_string()
                    );
                    assert_eq!(err.class_method, metalmq_codec::frame::CONNECTION_START_OK);

                    if let Some(ref c) = world.client {
                        c.close().await.unwrap();
                    }

                    world
                }),
            );

        builder
    }
}

#[tokio::main]
async fn main() {
    metalmq_client::setup_logger(log::LevelFilter::Debug);

    cucumber::Cucumber::<MyWorld>::new()
        .features(&["./features"])
        .steps(steps::steps())
        .cli()
        .run_and_exit()
        .await
}
