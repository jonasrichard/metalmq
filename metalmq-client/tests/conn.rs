#[macro_use]
extern crate lazy_static;

use metalmq_client::*;

lazy_static! {
    static ref LOG: () = env_logger::builder()
        //.filter_module("::metalmq_client", log::LevelFilter::Trace)
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
    assert_eq!(err.class_method, metalmq_codec::frame::CONNECTION_OPEN);

    Ok(())
}

#[cfg(feature = "integration-tests")]
#[tokio::test]
async fn double_open_the_same_channel() -> Result<()> {
    let c = connect(URL.into()).await?;
    c.open("/".into()).await?;

    c.channel_open(1).await?;

    let result = c.channel_open(1).await;

    assert!(result.is_err());

    let err = result.unwrap_err().downcast::<ClientError>().unwrap();
    assert_eq!(err.channel, Some(1));
    assert_eq!(err.code, 504);
    assert_eq!(err.class_method, metalmq_codec::frame::CONNECTION_OPEN);

    Ok(())
}

#[cfg(feature = "integration-tests")]
#[tokio::test]
async fn can_publish() -> Result<()> {
    helper("test2", "queue2").await?;

    Ok(())
}

#[cfg(feature = "integration-tests")]
async fn helper(exchange: &str, queue: &str) -> Result<Box<dyn Client>> {
    let c = connect(URL.into()).await?;
    c.open("/".into()).await?;
    c.channel_open(1).await?;
    c.exchange_declare(1, exchange, "fanout", None).await?;
    c.queue_declare(1, queue).await?;

    c.queue_bind(1, queue, exchange, "").await?;

    Ok(c)
}
