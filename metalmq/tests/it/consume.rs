//extern crate metalmq_client;

use super::helper;
use anyhow::Result;
use metalmq_codec::frame::{BasicConsumeFlags, ExchangeDeclareFlags};

#[tokio::test]
async fn consume_one_message() -> Result<()> {
    let exchange = "xchg-consume";
    let queue = "q-consume";
    let mut c = helper::default().connect().await?;

    let ch = c.channel_open(1).await?;
    helper::declare_exchange_queue(&ch, exchange, queue).await?;

    let result = helper::consume_messages(&ch, queue, "ctag", None, 1).await?;

    ch.basic_publish(exchange, "", "Hello".into(), false, false).await?;

    let msgs = result.await.unwrap();
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

    let res = helper::consume_messages(&ch, "not-existing-queue", "ctag", None, 1).await;

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

    //ch.queue_delete(queue, false, false).await?;
    //ch.exchange_delete(exchange, false).await?;

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

    let mut bc_flags = BasicConsumeFlags::empty();
    bc_flags |= BasicConsumeFlags::EXCLUSIVE;
    let res = helper::consume_messages(&ch, queue, "ctag", Some(bc_flags), 1).await;

    assert!(res.is_ok());

    let mut c2 = helper::default().connect().await?;

    let ch2 = c2.channel_open(3).await?;

    let result = helper::consume_messages(&ch2, queue, "ctag", Some(bc_flags), 1).await;

    println!("{:?}", result);

    assert!(result.is_err());

    let err = helper::to_client_error(result);
    assert_eq!(err.channel, Some(3));
    assert_eq!(err.code, 403);
    assert_eq!(err.class_method, metalmq_codec::frame::BASIC_CONSUME);

    Ok(())
}

//#[tokio::test]
//async fn three_consumers_consume_roughly_the_same_number_of_messages() -> Result<()> {
//    use std::collections::HashMap;
//    use std::sync::{Arc, Mutex};
//    use tokio::sync::broadcast;
//
//    let mut producer = helper::default().connect().await?;
//    let channel = producer.channel_open(12).await?;
//
//    channel.queue_delete("3-queue", false, false).await?;
//    channel.exchange_delete("3-exchange", false).await?;
//
//    helper::declare_exchange_queue(&channel, "3-exchange", "3-queue").await?;
//
//    let mut consumers = vec![];
//    let mut channels = vec![];
//    let total_message_count = Arc::new(Mutex::new(0u32));
//    let message_count = Arc::new(Mutex::new(HashMap::<u16, u32>::new()));
//    let (acked_tx, mut acked_rx) = broadcast::channel::<u32>(8);
//    let max = 30u32;
//
//    for i in 0..3u16 {
//        let mut consumer = helper::default().connect().await?;
//        let ch = consumer.channel_open(12).await?;
//        let msg_count = message_count.clone();
//        let total_count = total_message_count.clone();
//
//        message_count.lock().unwrap().insert(i, 0);
//
//        let counter = move |ci: ConsumerSignal| {
//            match ci {
//                ConsumerSignal::Delivered(m) => {
//                    // Count the messages per consumer number
//                    let mut mc = msg_count.lock().unwrap();
//                    if let Some(c) = mc.get_mut(&i) {
//                        *c += 1;
//                    }
//
//                    // Count the total number of messages
//                    let mut tc = total_count.lock().unwrap();
//                    *tc += 1;
//
//                    ConsumerResponse {
//                        result: None,
//                        ack: ConsumerAck::Ack {
//                            delivery_tag: m.delivery_tag,
//                            multiple: false,
//                        },
//                    }
//                }
//                _ => ConsumerResponse {
//                    result: Some(()),
//                    ack: ConsumerAck::Nothing,
//                },
//            }
//        };
//
//        ch.basic_consume("3-queue", &format!("ctag-{}", i), None, Box::new(counter))
//            .await?;
//
//        consumers.push(consumer);
//        channels.push(ch);
//    }
//
//    for i in 0..max {
//        channel
//            .basic_publish("3-exchange", "", format!("Message #{}", i))
//            .await?;
//    }
//
//    for i in 0..3usize {
//        channels.get(i).unwrap().basic_cancel(&format!("ctag-{}", i)).await?;
//    }
//
//    /*while let Ok(ack) = acked_rx.recv().await {
//        if ack == max {
//            break;
//        }
//    }*/
//
//    for cons in &mut consumers {
//        cons.close().await.unwrap();
//    }
//
//    for mc in message_count.lock().unwrap().iter() {
//        println!("Message count {:?}", mc);
//        assert!(mc.1 > &5u32 && mc.1 < &15u32);
//    }
//
//    producer.close().await?;
//
//    Ok(())
//}
