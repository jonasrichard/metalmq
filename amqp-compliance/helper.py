__name__ = "helper"

from contextlib import contextmanager
from typing import Optional

import pika
from pika.exchange_type import ExchangeType

def connect(
        username: str = "guest",
        password: str = "guest",
        host: str = "::1",
        port: int = 5672,
        vhost: str = "/"
) -> pika.BlockingConnection:
    """Open an AMQP connection.

    :param str username: Name of the user
    :param str password: Password in clear text
    :param str host: Name or IP addr of the server
    :param int port: Optionally the port
    :param str vhost: The path of the virtual host
    :rtype: pika.BlockingConnection 
    """
    return pika.BlockingConnection(
            pika.ConnectionParameters(
                host=host,
                port=port,
                virtual_host=vhost,
                credentials=pika.PlainCredentials(username, password)
                )
            )

@contextmanager
def connection():
    """
    Open a connection with the default parameters.
    """
    conn = connect()

    try:
        yield connect()
    finally:
        if conn.is_open:
            conn.close()

@contextmanager
def channel(number: Optional[int] = None):
    """
    Open a connection with the default parameters and open a channel with the specified channel
    number.

    :param int number: The number of the channel to be opened
    """
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
def direct_exchange(channel: pika.adapters.blocking_connection.BlockingChannel, exchange: str, *queues):
    try:
        channel.exchange_declare(exchange, exchange_type=ExchangeType.direct)
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
