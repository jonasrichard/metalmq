import helper
import logging
import pika
import threading
import time

LOG = logging.getLogger()
NUM = 3
end = threading.Condition()

def consumer_thread(channel):
    counter = 0

    def on_message(ch, method, properties, body):
        LOG.info("on_message %s", method)
        channel.basic_ack(delivery_tag=method.delivery_tag, multiple=False)

        counter += 1
        if counter == NUM:
            channel.stop_consuming()

            with end:
                end.notify()

    channel.basic_consume("speed1", on_message_callback=on_message)
    channel.start_consuming()

def test_one_publisher_one_consumer(caplog):
    publisher = helper.connect()
    pchan = publisher.channel(channel_number=9)

    pchan.exchange_declare(exchange="speed1")
    pchan.queue_declare("speed1")
    pchan.queue_bind("speed1", "speed1")

    consumer = helper.connect()
    channel = consumer.channel(channel_number=9)

    threading.Thread(target=consumer_thread, args=(channel,)).start()

    time.sleep(0.5)

    LOG.info("Start publishing")

    for i in range(0, NUM):
        channel.basic_publish(
                "speed1",
                "speed1",
                "Message body {}".format(i),
                pika.BasicProperties(content_type='text/plain', delivery_mode=1))

    with end:
        end.wait()

    LOG.info("End")
