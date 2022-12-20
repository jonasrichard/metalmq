import logging
import threading
import time

import pytest
import pika
import helper

LOG = logging.getLogger()
NUM = 10

def consumer_thread(channel, end):
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

    end = threading.Condition()
    threading.Thread(target=consumer_thread, args=(channel, end)).start()

    for i in range(0, NUM):
        pchan.basic_publish(
                "speed1",
                "speed1",
                "Message body {}".format(i),
                pika.BasicProperties(
                    content_type='text/plain',
                    delivery_mode=1,
                    content_encoding='utf-8',
                    message_id='id1'))

    LOG.info("End of publish")

    with end:
        end.wait()
    LOG.info("End")

def test_unrouted_mandatory_message():
    """
    If an exchange doesn't route messages to anywhere, or that specific message
    is not routed to any queue and mandatory is true, we need to send back an
    basic-return which results in an unroutable error.
    """
    def on_return(channel, method, props, body):
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
                properties=pika.BasicProperties(
                    content_type='text/plain',
                    content_encoding='utf-8',
                    message_id='id2',
                    timestamp=15440000,
                    delivery_mode=1),
                mandatory=True)

    assert exp.value.messages[0].method.reply_code == 312
    assert exp.value.messages[0].method.exchange == 'x-unroute'
    assert exp.value.messages[0].method.routing_key == 'routing-key'
    assert exp.value.messages[0].properties.content_type == 'text/plain'
    # Let us not test the delivery mode now
    #assert exp.value.messages[0].properties.delivery_mode == 1
    assert exp.value.messages[0].body == b"Unrouted message"

    pc.close()
    publisher.close()

def test_publish_too_long_message():
    def on_message(ch, method, props, body):
        None

    with pytest.raises(pika.exceptions.ChannelClosedByBroker) as exp:
        with helper.channel(1) as publishing_channel:
            publishing_channel.exchange_declare("x-too-long")

            body = "This is a long message. " * 5500
            publishing_channel.basic_publish("x-too-long", "*", body)

            threading.sleep(0.5)

    assert 311 == exp.value.reply_code
