use ironmq_client as client;

const URL: &str = "127.0.0.1:5672";

#[cfg(feature = "integration-tests")]
#[tokio::test]
async fn connect() -> client::Result<()> {
    let c = client::connect(URL.into()).await?;
    let result = client::open(&c, "/invalid".into()).await;

    assert!(result.is_err());

    let err = result.unwrap_err().downcast::<client::ClientError>().unwrap();
    assert_eq!(err.channel, None);
    assert_eq!(err.code, 530);
    assert_eq!(err.class_method, ironmq_codec::frame::CONNECTION_OPEN);

    Ok(())
}
