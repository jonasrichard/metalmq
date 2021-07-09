extern crate metalmq_client;

mod helper;

use anyhow::Result;
use metalmq_codec::frame::{BasicConsumeFlags, ExchangeDeclareFlags, QueueDeclareFlags};
use tokio::sync::oneshot;

#[tokio::test]
async fn consume_one_message() -> Result<()> {
    let exchange = "xchg-consume";
    let queue = "q-consume";
    let mut c = helper::default().connect().await?;

    let ch = c.channel_open(1).await?;
    helper::declare_exchange_queue(&ch, exchange, queue).await?;

    let (otx, orx) = oneshot::channel();

    helper::consume_messages(&ch, queue, "ctag", None, otx, 1).await?;

    ch.basic_publish(exchange, "", "Hello".into()).await?;

    let msgs = orx.await.unwrap();
    assert_eq!(msgs.len(), 1);

    let msg = msgs.get(0).unwrap();
    assert_eq!(msg.channel, 1);
    assert_eq!(msg.consumer_tag, "ctag");
    assert!(msg.delivery_tag > 0);
    assert_eq!(msg.length, 5);
    assert_eq!(msg.body, b"Hello");

    ch.close().await?;
    c.close().await?;

    Ok(())
}

#[tokio::test]
async fn consume_not_existing_queue() -> Result<()> {
    let mut c = helper::default().connect().await?;
    let ch = c.channel_open(2).await?;

    let (otx, _orx) = oneshot::channel();

    let res = helper::consume_messages(&ch, "not-existing-queue", "ctag", None, otx, 1).await;

    assert!(res.is_err());

    let err = helper::to_client_error(res);
    assert_eq!(err.channel, Some(2));
    assert_eq!(err.code, 404);
    assert_eq!(err.class_method, metalmq_codec::frame::BASIC_CONSUME);

    Ok(())
}

#[tokio::test]
async fn two_consumers_exclusive_queue_error() -> Result<()> {
    let exchange = "xchg-exclusive";
    let queue = "q-exclusive";
    let mut c = helper::default().connect().await?;

    let ch = c.channel_open(4).await?;

    let mut ex_flags = ExchangeDeclareFlags::empty();
    ex_flags |= ExchangeDeclareFlags::AUTO_DELETE;
    ch.exchange_declare(exchange, "direct", Some(ex_flags)).await?;

    // TODO write another test to get 405 - resource locker for consuming exclusive queue
    //
    //let mut q_flags = QueueDeclareFlags::empty();
    //q_flags |= QueueDeclareFlags::EXCLUSIVE;
    //ch.queue_declare(queue, Some(q_flags)).await?;
    ch.queue_declare(queue, None).await?;

    ch.queue_bind(queue, exchange, "").await?;

    let (otx, _orx) = oneshot::channel();

    let mut bc_flags = BasicConsumeFlags::empty();
    bc_flags |= BasicConsumeFlags::EXCLUSIVE;
    helper::consume_messages(&ch, queue, "ctag", Some(bc_flags), otx, 1).await?;

    let mut c2 = helper::default().connect().await?;

    let ch2 = c2.channel_open(3).await?;

    let (otx2, _orx2) = oneshot::channel();
    let result = helper::consume_messages(&ch2, queue, "ctag", Some(bc_flags), otx2, 1).await;

    println!("{:?}", result);

    assert!(result.is_err());

    let err = helper::to_client_error(result);
    assert_eq!(err.channel, Some(3));
    assert_eq!(err.code, 403);
    assert_eq!(err.class_method, metalmq_codec::frame::BASIC_CONSUME);

    Ok(())
}
