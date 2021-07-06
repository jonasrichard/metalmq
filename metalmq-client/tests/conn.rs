use anyhow::Result;
use metalmq_client::ClientError;

#[tokio::test]
async fn can_connect() -> Result<()> {
    let c = metalmq_client::connect("localhost:5672", "guest", "guest").await?;
    let result = c.open("/invalid".into()).await;

    assert!(result.is_err());

    let err = result.unwrap_err().downcast::<ClientError>().unwrap();
    assert_eq!(err.channel, None);
    assert_eq!(err.code, 530);
    assert_eq!(err.class_method, metalmq_codec::frame::CONNECTION_OPEN);

    Ok(())
}

#[tokio::test]
async fn double_open_the_same_channel() -> Result<()> {
    let mut c = metalmq_client::connect("localhost:5672", "guest", "guest").await?;
    c.open("/".into()).await?;

    let _ = c.channel_open(1).await?;

    let result = c.channel_open(1).await;

    assert!(result.is_err());

    let err = result.unwrap_err().downcast::<ClientError>().unwrap();
    assert_eq!(err.channel, None);
    assert_eq!(err.code, 504);
    assert_eq!(err.class_method, metalmq_codec::frame::CHANNEL_OPEN);

    Ok(())
}
