import logging
import pika

LOG = logging.getLogger()

def connect_as_guest():
    params = pika.URLParameters('amqp://guest:guest@localhost:5672/%2F')
    return pika.BlockingConnection(parameters=params)

def declare_exchange(conn):
    channel = conn.channel(channel_number=3)
    channel.exchange_declare(exchange='ctag-exchange')
    channel.queue_declare('ctag-queue')
    channel.queue_bind('ctag-queue', 'ctag-exchange')

    return channel

def cleanup(channel, exchange, queue):
    channel.queue_unbind(queue, exchange)
    channel.queue_delete(queue)
    channel.exchange_delete(exchange)

messages_received = 0

def on_receive(channel, method, properties, body):
    global messages_received

    LOG.info('Go message %s %s %s', method, properties, body)
    messages_received += 1
    channel.basic_ack(delivery_tag=method.delivery_tag, multiple=False)

    if messages_received == 10:
        channel.stop_consuming()


def test_server_generated_consumer_tags_one_by_one_ack(caplog):
    conn = connect_as_guest()
    channel = declare_exchange(conn)

    for i in range(0, 10):
        channel.basic_publish(
                'ctag-exchange',
                'ctag-queue',
                'Message {}'.format(i),
                pika.BasicProperties(content_type='text/plain', delivery_mode=1))

    channel.basic_consume('ctag-queue', on_message_callback=on_receive)
    channel.start_consuming()

    cleanup(channel, 'ctag-exchange', 'ctag-queue')
    conn.close()
