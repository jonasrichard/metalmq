__name__ = "helper"

from contextlib import contextmanager
import pika

def connect(username: str ="guest", password="guest", host="::1", port=5672, vhost="/"):
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
    ch = conn.channel(number)

    try:
        yield ch
    finally:
        if ch.is_open:
            ch.close()
        if conn.is_open:
            conn.close()

@contextmanager
def direct_exchange(channel, exchange, *queues):
    try:
        channel.exchange_declare(exchange, exchange_type="direct")
        for queue in queues:
            if isinstance(queue, tuple):
                (queue_name, routing_key) = queue
                channel.queue_declare(queue_name)
                channel.queue_bind(queue_name, exchange, routing_key)
            else:
                channel.queue_declare(queue)
                channel.queue_bind(queue, exchange, queue)

        yield
    finally:
        for queue in queues:
            if isinstance(queue, tuple):
                (queue_name, routing_key) = queue
                channel.queue_unbind(queue_name, exchange, routing_key)
                channel.queue_delete(queue_name)
            else:
                channel.queue_unbind(queue, exchange, queue)
                channel.queue_delete(queue)

        channel.exchange_delete(exchange)

@contextmanager
def topic_exchange(channel, exchange, *queues):
    try:
        channel.exchange_declare(exchange, exchange_type="topic")
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
            channel.queue_bind(queue, exchange, queue)

        yield
    finally:
        for queue in queues:
            channel.queue_unbind(queue, exchange, queue)
            channel.queue_delete(queue)

        channel.exchange_delete(exchange)
