import logging
import pika
import pytest
import threading
import time

LOG = logging.getLogger()
message_received = threading.Condition()

def connect_as_guest():
    params = pika.URLParameters('amqp://guest:guest@localhost:5672/%2F')
    return pika.BlockingConnection(parameters=params)

def declare_exchange_and_queue(conn):
    channel = conn.channel(channel_number=1)
    channel.exchange_declare('my-exchange',
            exchange_type='topic',
            passive=False,
            durable=False,
            auto_delete=False,
            internal=False)

    channel.queue_declare('my-queue')
    channel.queue_bind('my-queue', 'my-exchange')

    return channel


def publish_message(channel):
    LOG.info('Publishing message')
    channel.basic_publish('my-exchange',
            'my-queue',
            'Hey, receiver! How are you?',
            pika.BasicProperties(content_type='text/plain', delivery_mode=1))

def cleanup(channel):
    channel.queue_unbind('my-queue', 'my-exchange')
    channel.queue_delete('my-queue')
    channel.exchange_delete('my-exchange')

def consume_message(conn):
    channel = conn.channel(channel_number=1)
    channel.basic_consume('my-queue', on_message_callback=on_consumer_receive)
    channel.start_consuming()

def on_consumer_receive(channel, method, properties, body):
    LOG.info('Got message %s', body)
    channel.stop_consuming()

    with message_received:
        message_received.notify()

def test_basic_publish(caplog):
    """
    Send a message to the default exchange and the other user will get it.
    """
    caplog.set_level(logging.INFO)

    sender = connect_as_guest()
    channel = declare_exchange_and_queue(sender)

    receiver = connect_as_guest()
    threading.Thread(target=consume_message, args=(receiver,)).start()

    publish_message(channel)

    with message_received:
        message_received.wait()

    cleanup(channel)

    receiver.close()
    sender.close()

def test_publish_and_consume(caplog):
    """
    Send messages then receiver consumes then, test if server stores them.
    """
    caplog.set_level(logging.INFO)

    sender = connect_as_guest()
    channel = declare_exchange_and_queue(sender)

    publish_message(channel)

    receiver = connect_as_guest()
    threading.Thread(target=consume_message, args=(receiver,)).start()

    with message_received:
        message_received.wait()

    cleanup(channel)

    receiver.close()
    sender.close()
