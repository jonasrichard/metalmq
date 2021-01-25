use ironmq_client as client;
use ironmq_test::{init, step, Steps};

struct World {
    conn: Box<dyn client::Client>
}

#[cfg(feature = "integration-tests")]
#[tokio::test]
async fn publish_to_non_existing_exchange() {
    Steps
        ::feature("Publish to non-existing exchange", init!(World, {
            Ok(World {
                conn: client::connect("127.0.0.1:5672").await.unwrap()
            })
        })).await
        .given("a connection", step!(|w: World| {
            w.conn.open("/").await?;
            w.conn.channel_open(1).await?;
            Ok(())
        }))
        .when("publish a non-existing exchange", step!(|w: World| {
            Ok(())
        }))
        .then("it returns 404 error", step!(|w: World| {
            let res = w.conn.basic_publish(1, "non existent", "any key", "data".to_string()).await;

            assert!(res.is_err());

            Ok(())
        }))
        .check().await;
}

#[cfg(feature = "integration-tests")]
#[tokio::test]
async fn publish_to_default_exchange() -> client::Result<()> {
    let c = client::connect("127.0.0.1:5672").await?;
    c.open("/").await?;
    c.channel_open(1).await?;

    // TODO connect default exchange to a queue to see the message published there

    let result = c.basic_publish(1, "", "any key", "data".to_string()).await;

    assert!(result.is_ok());

    Ok(())
}

#[cfg(feature = "integration-tests")]
#[tokio::test]
async fn publish_to_intenal_exchange() -> client::Result<()> {
    let c = client::connect("127.0.0.1:5672").await?;
    c.open("/").await?;
    c.channel_open(1).await?;

    let mut flags = ironmq_codec::frame::ExchangeDeclareFlags::empty();
    flags |= ironmq_codec::frame::ExchangeDeclareFlags::INTERNAL;

    c.exchange_declare(1, "internal", "fanout", Some(flags)).await?;
    c.queue_bind(1, "normal-queue", "internal", "").await?;

    let result = c.basic_publish(1, "internal", "any key", "data".to_string()).await;

    assert!(result.is_err());

    let err = ironmq_test::to_client_error(result);
    assert_eq!(err.code, 403);

    Ok(())
}

// TODO test if exchange refuses basic content (how and why it does?)
