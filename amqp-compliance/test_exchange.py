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

def test_exchange_bind_nonexisting_queue():
    """
    Binding non-existing queue to a declared exchange.
    """
    with helper.channel(1) as channel:
        channel.exchange_declare("normal-exchange")

        with pytest.raises(pika.exceptions.ChannelClosedByBroker) as exp:
            channel.queue_bind("non-existent-queue", "normal-exchange", "*")

        assert 404 == exp.value.reply_code
