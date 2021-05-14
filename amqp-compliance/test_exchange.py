import logging
import pika
import pytest
import threading
import time

LOG = logging.getLogger()
queue_bound = threading.Condition()
message_published = threading.Condition()

def connect_as_guest():
    params = pika.URLParameters('amqp://guest:guest@localhost:5672/%2F')
    return pika.BlockingConnection(parameters=params)

def send_message():
    conn = connect_as_guest()
    channel = conn.channel(channel_number=1)
    channel.exchange_declare('my-exchange',
            exchange_type='topic',
            passive=False,
            durable=False,
            auto_delete=False,
            internal=False)

    channel.queue_declare('my-queue')
    channel.queue_bind('my-queue', 'my-exchange')

    with queue_bound:
        queue_bound.notify()

    LOG.info('Publishing message')
    channel.basic_publish('my-exchange',
            'my-queue',
            'Hey, receiver! How are you?',
            pika.BasicProperties(content_type='text/plain', delivery_mode=1))

    with message_published:
        message_published.wait()

    channel.queue_unbind('my-queue', 'my-exchange')
    channel.queue_delete('my-queue')
    channel.exchange_delete('my-exchange')

    conn.close()

def consume_message():
    with queue_bound:
        queue_bound.wait()
    conn = connect_as_guest()
    channel = conn.channel(channel_number=1)
    channel.basic_consume('my-queue', on_message_callback=on_consumer_receive)
    channel.start_consuming()

def on_consumer_receive(channel, method, properties, body):
    LOG.info('Got message %s', body)
    channel.stop_consuming()

    with message_published:
        message_published.notify()

def test_basic_publish(caplog):
    """
    Send a message to the default exchange and the other user will get it.
    """
    caplog.set_level(logging.INFO)

    receiver = threading.Thread(target=consume_message)
    receiver.start()

    sender = threading.Thread(target=send_message)
    sender.start()

    sender.join()
    receiver.join()
