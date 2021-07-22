import helper
import logging
import pika
import pytest
import threading
import time

LOG = logging.getLogger()
NUM = 1000
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

def test_unrouted_mandatory_message(caplog):
    def on_return(channel, method, props, body):
        breakpoint()
        LOG.info("Return %s %s %s", method, props, body)

    publisher = helper.connect()
    pc = publisher.channel(channel_number=13)
    pc.exchange_declare(exchange="x-unroute")
    pc.add_on_return_callback(on_return)
    pc.confirm_delivery()

    with pytest.raises(pika.exceptions.UnroutableError) as exp:
        pc.basic_publish(
                "x-unroute",
                "routing-key",
                "Unrouted message",
                properties=pika.BasicProperties(content_type='text/plain', delivery_mode=1),
                mandatory=True)

    assert exp.value.messages[0].method.reply_code == 312
    assert exp.value.messages[0].method.exchange == 'x-unroute'
    assert exp.value.messages[0].method.routing_key == 'routing-key'
    assert exp.value.messages[0].properties.content_type == 'text/plain'
    assert exp.value.messages[0].properties.delivery_mode == 1
    assert exp.value.messages[0].body == b"Unrouted message"

    pc.close()
    publisher.close()
