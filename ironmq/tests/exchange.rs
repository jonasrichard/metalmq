use ironmq_client as client;
use ironmq_test::{step, Steps};

#[derive(Default)]
struct World {
    conn: Option<Box<dyn client::Client>>
}

#[tokio::test]
async fn first() {
    Steps
        ::feature("Connect to the virtual host /")
        .given("a connection", step!(|w: World| {
            let c = client::connect("127.0.0.1:5672").await?;
            w.conn = Some(c);
            Ok(())
        }))
        .when("open to virtual host /", step!(|w: World| {
            w.conn.as_ref().unwrap().open("/").await?;
            Ok(())
        }))
        .then("we are connected", step!(|w: World| {
            Ok(())
        }))
        .check().await;
}

#[cfg(feature = "integration-tests")]
#[tokio::test]
async fn channel_close_on_not_existing_exchange() -> client::Result<()> {
    let c = client::connect("127.0.0.1:5672").await?;

    let mut flags = ironmq_codec::frame::ExchangeDeclareFlags::empty();
    flags |= ironmq_codec::frame::ExchangeDeclareFlags::PASSIVE;

    let result = c.exchange_declare(1, "sure do not exist", "fanout", Some(flags)).await;

    assert!(result.is_err());

    let err = helper::conn::to_client_error(result);
    assert_eq!(err.code, 404);

    Ok(())
}

#[cfg(feature = "integration-tests")]
#[tokio::test]
async fn passive_exchange_declare_check_if_exchange_exist() -> client::Result<()> {
    let c = client::connect("127.0.0.1:5672").await?;

    let mut flags = ironmq_codec::frame::ExchangeDeclareFlags::empty();
    c.exchange_declare(1, "new channel", "fanout", Some(flags)).await?;

    flags |= ironmq_codec::frame::ExchangeDeclareFlags::PASSIVE;
    let result = c.exchange_declare(1, "new channel", "fanout", Some(flags)).await;

    assert!(result.is_ok());

    Ok(())
}
