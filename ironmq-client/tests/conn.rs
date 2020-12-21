#[macro_use]
extern crate lazy_static;

use ironmq_client::*;

lazy_static! {
    static ref LOG: () = env_logger::builder()
        //.filter_module("::ironmq_client", log::LevelFilter::Trace)
        .is_test(true)
        .try_init()
        .unwrap();
}

const URL: &str = "127.0.0.1:5672";

#[cfg(feature = "integration-tests")]
#[tokio::test]
async fn can_connect() -> Result<()> {
    let c = connect(URL.into()).await?;
    let result = c.open("/invalid".into()).await;

    assert!(result.is_err());

    let err = result.unwrap_err().downcast::<ClientError>().unwrap();
    assert_eq!(err.channel, None);
    assert_eq!(err.code, 530);
    assert_eq!(err.class_method, ironmq_codec::frame::CONNECTION_OPEN);

    Ok(())
}

#[cfg(feature = "integration-tests")]
#[tokio::test]
async fn can_publish() -> Result<()> {
    helper("test2", "queue2").await?;

    Ok(())
}

async fn helper(exchange: &str, queue: &str) -> Result<Box<dyn Client>> {
    let c = connect(URL.into()).await?;
    c.open("/".into()).await?;
    c.channel_open(1).await?;
    c.exchange_declare(1, exchange, "fanout", None).await?;
    c.queue_declare(1, queue).await?;

    c.queue_bind(1, queue, exchange, "").await?;

    Ok(c)
}
