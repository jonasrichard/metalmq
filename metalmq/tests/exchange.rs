use metalmq_client as client;
use metalmq_client::{init, step, bdd::Steps};

#[cfg(feature = "integration-tests")]
struct World {
    conn: client::Client,
    result: client::Result<()>
}

#[cfg(feature = "integration-tests")]
#[tokio::test]
async fn first() {
    Steps
        ::feature("Connect to the virtual host /", init!(World, {
            Ok(World {
                conn: client::connect("127.0.0.1:5672").await?,
                result: Ok(())
            })
        })).await
        .given("a connection", step!(|w: World| {
            Ok(())
        }))
        .when("open to virtual host /", step!(|w: World| {
            w.conn.open("/").await
        }))
        .then("we are connected", step!(|w: World| {
            Ok(())
        }))
        .check().await;
}

#[cfg(feature = "integration-tests")]
#[tokio::test]
async fn channel_close_on_not_existing_exchange() {
    Steps
        ::feature("Channel closes on passive declare of non-existing exchange", init!(World, {
            Ok(World {
                conn: client::connect("127.0.0.1:5672").await?,
                result: Ok(())
            })
        })).await
        .given("a connection", step!(|w: World| {
            w.conn.open("/").await?;
            w.conn.channel_open(1).await
        }))
        .when("passive declare a non-existing exchange", step!(|w: World| {
            let mut flags = metalmq_codec::frame::ExchangeDeclareFlags::empty();
            flags |= metalmq_codec::frame::ExchangeDeclareFlags::PASSIVE;

            w.result = w.conn.exchange_declare(1, "sure does not exist", "fanout", Some(flags)).await;

            Ok(())
        }))
        .then("we get a channel error", step!(|w: World| {
            let mut r = Ok(());
            std::mem::swap(&mut w.result, &mut r);

            assert!(r.is_err());

            let err = metalmq_client::bdd::to_client_error(r);
            assert_eq!(err.code, 404);

            Ok(())
        }))
        .check().await;
}

#[cfg(feature = "integration-tests")]
#[tokio::test]
async fn passive_exchange_declare_check_if_exchange_exist() {
    Steps
        ::feature("Passive exchange declare check if exchange exists", init!(World, {
            Ok(World {
                conn: client::connect("127.0.0.1:5672").await?,
                result: Ok(())
            })
        })).await
        .given("an exchange declared", step!(|w: World| {
            let mut flags = metalmq_codec::frame::ExchangeDeclareFlags::empty();
            w.conn.exchange_declare(1, "new channel", "fanout", Some(flags)).await
        }))
        .when("declare the same exchange passively", step!(|w: World| {
            let mut flags = metalmq_codec::frame::ExchangeDeclareFlags::empty();
            flags |= metalmq_codec::frame::ExchangeDeclareFlags::PASSIVE;

            w.result = w.conn.exchange_declare(1, "new channel", "fanout", Some(flags)).await;
            Ok(())
        }))
        .then("it will succeed", step!(|w: World| {
            let mut r = Ok(());
            std::mem::swap(&mut w.result, &mut r);

            assert!(r.is_ok());
            Ok(())
        }))
        .check().await;
}
