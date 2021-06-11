import helper
import logging
import pika
import threading
import time

LOG = logging.getLogger()
NUM = 10
end = threading.Condition()

def consumer_thread(channel):
    counter = 0

    for method, props, body in channel.consume("speed1"):
        channel.basic_ack(method.delivery_tag)

        counter += 1
        if counter == NUM:
            break

    channel.cancel()
    with end:
        end.notify()
    channel.close()

def test_one_publisher_one_consumer(caplog):
    publisher = helper.connect()
    pchan = publisher.channel(channel_number=9)

    pchan.exchange_declare(exchange="speed1")
    pchan.queue_declare("speed1")
    pchan.queue_bind("speed1", "speed1")

    consumer = helper.connect()
    channel = consumer.channel(channel_number=9)

    threading.Thread(target=consumer_thread, args=(channel,)).start()

    for i in range(0, NUM):
        pchan.basic_publish(
                "speed1",
                "speed1",
                "Message body {}".format(i),
                pika.BasicProperties(content_type='text/plain', delivery_mode=1))

    LOG.info("End of publish")

    with end:
        end.wait()
    LOG.info("End")
