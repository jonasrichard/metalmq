use std::time::Duration;

use crate::{helper, unwrap_delivered_message};
use metalmq_client::*;

#[tokio::test]
async fn test_routing_logic() {
    let (mut client, _) = helper::connect().await.unwrap();
    let mut channel = client.channel_open(10u16).await.unwrap();

    channel
        .exchange_declare("files", ExchangeType::Topic, ExchangeDeclareOpts::default())
        .await
        .unwrap();
    channel
        .queue_declare("text-files", QueueDeclareOpts::default())
        .await
        .unwrap();
    channel
        .queue_declare("image-files", QueueDeclareOpts::default())
        .await
        .unwrap();
    channel
        .queue_declare("executable-files", QueueDeclareOpts::default())
        .await
        .unwrap();
    channel
        .queue_bind("text-files", "files", Binding::Topic("txt".to_string()))
        .await
        .unwrap();
    channel
        .queue_bind("text-files", "files", Binding::Topic("pdf".to_string()))
        .await
        .unwrap();
    channel
        .queue_bind("image-files", "files", Binding::Topic("png".to_string()))
        .await
        .unwrap();
    channel
        .queue_bind("executable-files", "files", Binding::Topic("exe".to_string()))
        .await
        .unwrap();
    channel.queue_purge("text-files").await.unwrap();
    channel.queue_purge("image-files").await.unwrap();
    channel.queue_purge("executable-files").await.unwrap();

    publish_deliver_message_to_proper_queue(&mut client).await;

    channel.queue_unbind("text-files", "files", "txt").await.unwrap();
    channel.queue_unbind("text-files", "files", "pdf").await.unwrap();
    channel.queue_unbind("image-files", "files", "png").await.unwrap();
    channel.queue_unbind("executable-files", "files", "exe").await.unwrap();
    channel
        .queue_delete("text-files", IfUnused(false), IfEmpty(false))
        .await
        .unwrap();
    channel
        .queue_delete("image-files", IfUnused(false), IfEmpty(false))
        .await
        .unwrap();
    channel
        .queue_delete("executable-files", IfUnused(false), IfEmpty(false))
        .await
        .unwrap();
    channel.exchange_delete("files", IfUnused(false)).await.unwrap();

    channel.close().await.unwrap();
    client.close().await.unwrap();
}

async fn publish_deliver_message_to_proper_queue(client: &mut Client) {
    let timeout = Duration::from_secs(1);
    let mut text = client.channel_open(11u16).await.unwrap();

    let mut text_handler = text
        .basic_consume("text-files", NoAck(false), Exclusive(false), NoLocal(false))
        .await
        .unwrap();

    let mut image = client.channel_open(12u16).await.unwrap();

    let mut image_handler = image
        .basic_consume("image-files", NoAck(false), Exclusive(false), NoLocal(false))
        .await
        .unwrap();

    let mut exec = client.channel_open(13u16).await.unwrap();

    let mut exec_handler = exec
        .basic_consume("executable-files", NoAck(false), Exclusive(false), NoLocal(false))
        .await
        .unwrap();

    let routing_keys = vec!["exe", "pdf", "txt", "png"];
    for routing_key in routing_keys {
        text.basic_publish(
            "files",
            routing_key,
            PublishedMessage::default().text(routing_key).channel(text.channel),
        )
        .await
        .unwrap();
    }

    let expected_exe = exec_handler.receive(timeout).await.unwrap();
    let exe_msg = unwrap_delivered_message(expected_exe);
    assert_eq!(exe_msg.consumer_tag, exec_handler.consumer_tag);
    assert_eq!(exe_msg.message.body, "exe".as_bytes());
    exec_handler.basic_ack(exe_msg.delivery_tag).await.unwrap();

    let expected_pdf = text_handler.receive(timeout).await.unwrap();
    let pdf_msg = unwrap_delivered_message(expected_pdf);
    assert_eq!(pdf_msg.consumer_tag, text_handler.consumer_tag);
    assert_eq!(pdf_msg.message.body, "pdf".as_bytes());
    text_handler.basic_ack(pdf_msg.delivery_tag).await.unwrap();

    let expected_txt = text_handler.receive(timeout).await.unwrap();
    let txt_msg = unwrap_delivered_message(expected_txt);
    assert_eq!(txt_msg.consumer_tag, text_handler.consumer_tag);
    assert_eq!(txt_msg.message.body, "txt".as_bytes());
    text_handler.basic_ack(txt_msg.delivery_tag).await.unwrap();

    let expected_png = image_handler.receive(timeout).await.unwrap();
    let png_msg = unwrap_delivered_message(expected_png);
    assert_eq!(png_msg.consumer_tag, image_handler.consumer_tag);
    assert_eq!(png_msg.message.body, "png".as_bytes());
    image_handler.basic_ack(png_msg.delivery_tag).await.unwrap();

    text_handler.basic_cancel().await.unwrap();
    image_handler.basic_cancel().await.unwrap();
    exec_handler.basic_cancel().await.unwrap();
    text.close().await.unwrap();
    image.close().await.unwrap();
    exec.close().await.unwrap();
}
