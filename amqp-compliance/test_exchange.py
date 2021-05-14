import logging
import pika
import pytest
import threading

def connect_as_guest():
    params = pika.URLParameters('amqp://guest:guest@localhost:5672/%2F')
    return pika.BlockingConnection(parameters=params)

def send_message():
    conn = connect_as_guest()
    channel = conn.channel(channel_number=1)
    channel.exchange_declare('my-exchange',
            exchange_type='direct',
            passive=False,
            durable=False,
            auto_delete=False,
            internal=False)

    channel.queue_declare('my-queue')
    channel.queue_bind('my-queue', 'my-exchange')

    logging.getLogger().info('Start publishing')
    channel.basic_publish('my-exchange',
            'routing-key',
            'Hey, receiver! How are you?',
            pika.BasicProperties(content_type='text/plain', delivery_mode=1))

def consume_message():
    conn = connect_as_guest()
    channel = conn.channel(channel_number=1)
    channel.basic_consume('my-queue', on_message_callback=on_consumer_receive)
    logging.getLogger().info('start consuming')
    channel.start_consuming()

def on_consumer_receive(channel, method, properties, body):
    print(body)
    breakpoint()
    channel.stop_consuming()

def test_basic_publish(caplog):
    """
    Send a message to the default exchange and the other user will get it.
    """
    caplog.set_level(logging.INFO)

    receiver = threading.Thread(target=consume_message())
    receiver.start()

    sender = threading.Thread(target=send_message())
    sender.start()

    sender.join()
    receiver.join()
