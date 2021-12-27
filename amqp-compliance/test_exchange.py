import helper
import logging
import pika
import pytest
import threading
import time

LOG = logging.getLogger()
message_received = threading.Condition()

def declare_exchange_and_queue(conn, exchange, exchange_type, queue):
    channel = conn.channel(channel_number=1)
    channel.exchange_declare(exchange=exchange,
            exchange_type=exchange_type,
            passive=False,
            durable=False,
            auto_delete=False,
            internal=False)

    channel.queue_declare(queue)
    channel.queue_bind(queue, exchange)

    return channel


def publish_message(channel, exchange, routing_key):
    LOG.info('Publishing message')
    channel.basic_publish(exchange,
            routing_key,
            'Hey, receiver! How are you?',
            pika.BasicProperties(content_type='text/plain', delivery_mode=1))

def cleanup(channel, exchange, queue):
    channel.queue_unbind(queue, exchange)
    channel.queue_delete(queue)
    channel.exchange_delete(exchange)

def consume_message(conn, queue):
    channel = conn.channel(channel_number=1)
    channel.basic_consume(queue, on_message_callback=on_consumer_receive)
    channel.start_consuming()

def on_consumer_receive(channel, method, properties, body):
    LOG.info('Got message %s', body)

    channel.basic_ack(delivery_tag=method.delivery_tag, multiple=False)
    channel.stop_consuming()

    with message_received:
        message_received.notify()

def test_basic_publish(caplog):
    """
    Send a message to the default exchange and the other user will get it.
    """
    sender = helper.connect()
    channel = declare_exchange_and_queue(sender, 'my-exchange', 'topic', 'my-queue')

    receiver = helper.connect()
    threading.Thread(target=consume_message, args=(receiver, 'my-queue')).start()

    # Give chance to the consumer for starting and going into waiting state
    time.sleep(0.5)

    publish_message(channel, 'my-exchange', 'my-queue')

    with message_received:
        message_received.wait()

    cleanup(channel, 'my-exchange', 'my-queue')

    receiver.close()
    sender.close()

def test_exchange_mandatory_error(caplog):
    """
    Basic return should send back if messages is non-routable and mandatory is true
    """
    client = helper.connect()
    channel = client.channel(channel_number=4)

    channel.confirm_delivery()
    channel.exchange_declare(exchange='not-routed', exchange_type='topic')

    with pytest.raises(pika.exceptions.UnroutableError) as exp:
        channel.basic_publish('not-routed', 'any', 'body', mandatory=True)

    channel.exchange_delete('not-routed')
    channel.close()
    client.close()

def test_direct_exchange_two_queues(caplog):
    caplog.set_level(logging.INFO)
    messages = []

    def consume(queue_name):
        def on_message(channel, method, props, body):
            LOG.info("consume %s", body)
            messages.append(body)
            channel.basic_ack(method.delivery_tag)
            channel.basic_cancel("ct1")

        consumer = helper.connect()
        consumer_channel = consumer.channel()

        LOG.info("Sending basic consume %s", queue_name)
        consumer_channel.basic_consume(queue_name, on_message_callback=on_message, consumer_tag="ct1")
        consumer_channel.start_consuming()

        LOG.info("After start consuming")

        consumer_channel.close()
        consumer.close()

    client = helper.connect()
    channel = client.channel(channel_number=8)

    channel.exchange_declare(exchange="2-exchange", exchange_type="direct")
    channel.queue_declare("1-queue")
    channel.queue_bind("1-queue", "2-exchange", "1-queue")
    channel.queue_declare("2-queue")
    channel.queue_bind("2-queue", "2-exchange", "2-queue")

    consumer1 = threading.Thread(target=consume, args=("1-queue",))
    consumer1.start()

    channel.basic_publish("2-exchange", "1-queue", "This goes to the first queue")

    consumer1.join()
    channel.queue_unbind("1-queue", "2-exchange", "1-queue")
    channel.queue_unbind("2-queue", "2-exchange", "2-queue")
    channel.exchange_delete("2-exchange")
    channel.queue_delete("1-queue")
    channel.queue_delete("2-queue")

    channel.close()
    client.close()

    assert 1 == len(messages)
    LOG.info("Messages %r", messages[0])
