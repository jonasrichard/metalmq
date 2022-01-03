__name__ = "helper"

from contextlib import contextmanager
import pika

def connect(username="guest", password="guest", host="localhost", port=5672, vhost="/"):
    return pika.BlockingConnection(
            pika.ConnectionParameters(
                host=host,
                port=port,
                virtual_host=vhost,
                credentials=pika.PlainCredentials(username, password)
                )
            )

@contextmanager
def channel(number=None):
    conn = connect()
    channel = conn.channel(number)

    try:
        yield channel
    finally:
        channel.close()
        conn.close()

@contextmanager
def direct_exchange(channel, exchange, *queues):
    try:
        channel.exchange_declare(exchange, exchange_type="direct")
        for queue in queues:
            match queue:
                case (queue_name, routing_key):
                    channel.queue_declare(queue_name)
                    channel.queue_bind(queue_name, exchange, routing_key)
                case queue_name:
                    channel.queue_declare(queue_name)
                    channel.queue_bind(queue_name, exchange, queue_name)

        yield
    finally:
        for queue in queues:
            match queue:
                case (queue_name, routing_key):
                    channel.queue_unbind(queue_name, exchange, routing_key)
                    channel.queue_delete(queue_name)
                case queue_name:
                    channel.queue_unbind(queue_name, exchange, queue_name)
                    channel.queue_delete(queue_name)

        channel.exchange_delete(exchange)

@contextmanager
def topic_exchange(channel, exchange, *queues):
    try:
        channel.exchange_declare(exchange, exchange_type="direct")
        for (queue_name, routing_key) in queues:
            channel.queue_declare(queue_name)
            channel.queue_bind(queue_name, exchange, routing_key)

        yield
    finally:
        for (queue_name, routing_key) in queues:
            channel.queue_unbind(queue_name, exchange, routing_key)
            channel.queue_delete(queue_name)

        channel.exchange_delete(exchange)

@contextmanager
def fanout_exchange(channel, exchange, *queues):
    try:
        channel.exchange_declare(exchange, exchange_type="fanout")

        for queue in queues:
            channel.queue_declare(queue)
            channel.queue_bind(queue, exchange, queues)

        yield
    finally:
        for queue in queues:
            channel.queue_unbind(queue, exchange, queues)
            channel.queue_delete(queue)

        channel.exchange_delete(exchange)
