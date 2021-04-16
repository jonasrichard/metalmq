use anyhow::Result;
use metalmq_client as client;
use metalmq_client::{bdd::Steps, init, step};

struct World {
    conn: client::Client,
    result: Result<()>,
}

#[cfg(feature = "integration-tests")]
#[tokio::test]
async fn publish_to_non_existing_exchange() {
    Steps::feature(
        "Publish to non-existing exchange",
        init!(World, {
            Ok(World {
                conn: client::connect("127.0.0.1:5672").await?,
                result: Ok(()),
            })
        }),
    )
    .await
    .given(
        "a connection",
        step!(|w: World| {
            w.conn.open("/").await?;
            w.conn.channel_open(1).await?;
            Ok(())
        }),
    )
    .when(
        "publish a non-existing exchange",
        step!(|w: World| {
            w.result = w
                .conn
                .basic_publish(1, "non existent", "any key", "data".to_string())
                .await;
            Ok(())
        }),
    )
    .then(
        "it returns 404 error",
        step!(|w: World| {
            println!("{:?}", w.result);
            let mut res = Ok(());
            std::mem::swap(&mut w.result, &mut res);

            assert!(res.is_err());
            Ok(())
        }),
    )
    .check()
    .await;
}

#[cfg(feature = "integration-tests")]
#[tokio::test]
async fn publish_to_default_exchange() {
    Steps::feature(
        "Publish to default exchange",
        init!(World, {
            Ok(World {
                conn: client::connect("127.0.0.1:5672").await?,
                result: Ok(()),
            })
        }),
    )
    .await
    .given(
        "a connection",
        step!(|w: World| {
            w.conn.open("/").await?;
            w.conn.channel_open(1).await
        }),
    )
    .when(
        "publish a message with empty channel",
        step!(|w: World| {
            w.result = w.conn.basic_publish(1, "", "any key", "data".to_string()).await;
            Ok(())
        }),
    )
    .then(
        "it goes to the default exchange",
        step!(|w: World| {
            assert!(w.result.is_ok());
            Ok(())
        }),
    )
    .check()
    .await;
}

#[cfg(feature = "integration-tests")]
#[tokio::test]
async fn publish_to_intenal_exchange() {
    Steps::feature(
        "Publish to internal exchange",
        init!(World, {
            Ok(World {
                conn: client::connect("127.0.0.1:5672").await?,
                result: Ok(()),
            })
        }),
    )
    .await
    .given(
        "a connection",
        step!(|w: World| {
            w.conn.open("/").await?;
            w.conn.channel_open(1).await
        }),
    )
    .when(
        "publish to an internal exchange",
        step!(|w: World| {
            let mut flags = metalmq_codec::frame::ExchangeDeclareFlags::empty();
            flags |= metalmq_codec::frame::ExchangeDeclareFlags::INTERNAL;

            w.conn.exchange_declare(1, "internal", "fanout", Some(flags)).await?;
            w.conn.queue_bind(1, "normal-queue", "internal", "").await?;

            w.result = w.conn.basic_publish(1, "internal", "any key", "data".to_string()).await;
            Ok(())
        }),
    )
    .then(
        "it gives a 403 channel error",
        step!(|w: World| {
            let mut res = Ok(());
            std::mem::swap(&mut w.result, &mut res);
            assert!(res.is_err());

            let err = metalmq_client::bdd::to_client_error(res);
            assert_eq!(err.code, 403);

            Ok(())
        }),
    )
    .check()
    .await;
}

// TODO test if exchangerefuses basic content (how and why it does?)
