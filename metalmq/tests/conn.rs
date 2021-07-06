extern crate metalmq_client;

mod helper;

#[tokio::test]
async fn connect() -> anyhow::Result<()> {
    let _ = helper::default().connect().await?;

    Ok(())
}

#[tokio::test]
async fn open_already_opened_channel() -> anyhow::Result<()> {
    let mut c = helper::default().connect().await?;

    let _ = c.channel_open(1).await?;

    let result = c.channel_open(1).await;

    assert!(result.is_err());

    let err = helper::to_client_error(result);

    assert_eq!(err.code, 504);

    Ok(())
}
