import helper
import pika
import threading
import time

def publisher(channel, consumer_channel):
    for i in range(0, 3):
        pchan.basic_publish(
                "speed1",
                "speed1",
                "Message body {}".format(i),
                pika.BasicProperties(content_type='text/plain', delivery_mode=1))

    consumer_channel.stop_consuming()

def test_one_publisher_one_consumer(caplog):
    publisher = helper.connect()
    pchan = publisher.channel(channel_number=9)

    pchan.exchange_declare(exchange="speed1")
    pchan.queue_declare("speed1")
    pchan.queue_bind("speed1", "speed1")

    def on_message(channel, method, properties, body):
        channel.basic_ack(delivery_tag=method.delivery_tag, multiple=False)

    consumer = helper.connect()
    channel = consumer.channel(channel_number=9)
    channel.basic_consume("speed1", on_message_callback=on_message)

    time.sleep(0.5)

    threading.Thread(target=publisher, args=(pchan, channel)).start()

    channel.start_consuming()
