extern crate metalmq_client;

mod helper;

use anyhow::Result;
use metalmq_codec::frame::{BasicConsumeFlags, ExchangeDeclareFlags, QueueDeclareFlags};
use tokio::sync::{mpsc, oneshot};

#[ignore]
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

#[ignore]
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

#[ignore]
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

#[tokio::test]
async fn three_consumers_consume_roughly_the_same_number_of_messages() -> Result<()> {
    let mut producer = helper::default().connect().await?;
    let channel = producer.channel_open(12).await?;

    helper::declare_exchange_queue(&channel, "3-exchange", "3-queue").await?;

    let mut consumers = vec![];
    let mut join_handles = vec![];
    let message_count = std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashMap::<u16, u32>::new()));

    for i in 0..3u16 {
        message_count.lock().unwrap().insert(i, 0);

        let mut consumer = helper::default().connect().await?;

        let ch = consumer.channel_open(12).await?;

        let (msg_tx, mut msg_rx) = mpsc::channel::<metalmq_client::Message>(1);
        let bc = ch.consumer();

        let msg_count = message_count.clone();

        let jh = tokio::spawn(async move {
            while let Some(msg) = msg_rx.recv().await {
                println!("{} {:?}", i, msg);

                bc.basic_ack(msg.delivery_tag).await.unwrap();

                let mut mc = msg_count.lock().unwrap();
                if let Some(cnt) = mc.get_mut(&i) {
                    *cnt += 1;
                }
            }
        });

        ch.basic_consume("3-queue", &format!("ctag-{}", i), None, msg_tx)
            .await?;

        consumers.push(consumer);
        join_handles.push(jh);
    }

    for i in 0..6u16 {
        channel
            .basic_publish("3-exchange", "", format!("Message #{}", i))
            .await?;
    }

    //for jh in join_handles {
    //    jh.await.unwrap();
    //}

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    for cons in &consumers {
        cons.close().await.unwrap();
    }

    for mc in message_count.lock().unwrap().iter() {
        println!("Message count {:?}", mc);
        assert!(mc.1 > &1u32);
    }

    producer.close().await?;

    Ok(())
}
