import datetime
import logging
import pika

import helper

LOG = logging.getLogger()


class PublishConsumeTest():
    """Convenience class for publish consume tests"""

    def __init__(
            self,
            channel_number=1,
            exchange_name="test-exchange",
            queue_name="test-queue"
    ):
        self.exchange_name = exchange_name
        self.queue_name = queue_name

        self.connection = helper.connect()
        self.channel = self.connection.channel(channel_number=channel_number)

    def declare_exchange(self):
        """Declare exchange and binds a queue to it with default routing key"""
        self.channel.exchange_declare(exchange=self.exchange_name)
        self.channel.queue_declare(self.queue_name)
        self.channel.queue_bind(self.queue_name, self.exchange_name)

    def close(self, without_cleanup=False):
        """Close connection, with cleanup it deletes exchanges and queues"""
        if not without_cleanup:
            self.channel.queue_unbind(self.queue_name, self.exchange_name)
            self.channel.queue_delete(self.queue_name)
            self.channel.exchange_delete(self.exchange_name)

        self.channel.close()
        self.connection.close()

    def publish(self, body):
        """Publish a message with body to the exchange"""
        self.channel.basic_publish(
            self.exchange_name,
            self.queue_name,
            body,
            pika.BasicProperties(content_type='text/plain', delivery_mode=1))

    def consume(self, on_message_delivered):
        """Start consuming using the callback for on message delivered"""
        self.channel.basic_consume(
            self.queue_name,
            on_message_callback=lambda ch, method, props, body:
            on_message_delivered(self, ch, method, props, body))
        self.channel.start_consuming()

    def cancel_consume(self):
        """Cancel consuming the queue"""
        self.channel.stop_consuming()


def test_xyz():
    """
    TODO this will test the redelivery of un-acked messages. We publish
    messages in an exchange, consume without acking and closing the connection.
    Reconnect and consuming again, and receive the messages.
    """
    expected_body = datetime.datetime.now().strftime("%H:%M:%S.%f")

    def on_message(obj, _channel, _method, _props, body):
        LOG.info("on_msg %s", body)
        assert body.decode("utf-8") == expected_body

        obj.cancel_consume()

    t = PublishConsumeTest(
        channel_number=3,
        exchange_name='ctag-exchange',
        queue_name='ctag_queue')
    t.declare_exchange()
    t.publish(expected_body)
    t.consume(on_message)
    t.close(without_cleanup=True)


def test_server_generated_consumer_tags_one_by_one_ack(caplog):
    """
    Publish messages and acking them one by one.
    """
    exchange_name = 'ctag-exchange'
    queue_name = 'ctag-queue'

    messages_received = 0
    last_delivery_tag = -1

    def on_message(obj, channel, method, props, body):
        nonlocal messages_received
        nonlocal last_delivery_tag

        LOG.info('Got message %s %s %s', method, props, body)
        messages_received += 1

        #assert method.consumer_tag == current_consumer_tag

        assert last_delivery_tag < method.delivery_tag
        last_delivery_tag = method.delivery_tag

        #assert not (method.redelivered)

        assert method.routing_key == 'q-ctag-ack'

        channel.basic_ack(delivery_tag=method.delivery_tag, multiple=False)

        if messages_received == 10:
            channel.stop_consuming()

    t = PublishConsumeTest(
        channel_number=7,
        exchange_name='xchg-ctag-ack',
        queue_name='q-ctag-ack')
    t.declare_exchange()

    for i in range(0, 10):
        t.publish('Message {}'.format(i))

    # TODO how to get the consumer-tag of the consuming?
    t.consume(on_message)
    t.close()


def test_publish_and_then_consume(caplog):
    received_messages = 0

    def on_message(obj, channel, method, props, body):
        nonlocal received_messages

        LOG.info("Late message %s %s %s", method, props, body)
        channel.basic_ack(delivery_tag=method.delivery_tag, multiple=False)
        received_messages += 1

        if received_messages == 10:
            obj.cancel_consume()

    s = PublishConsumeTest(
        channel_number=6,
        exchange_name='xchg-late-consume',
        queue_name='q-late-consume')
    s.declare_exchange()

    for i in range(0, 10):
        s.publish('Message {}'.format(i))

    s.close(without_cleanup=True)

    c = PublishConsumeTest(
        channel_number=6,
        exchange_name='xchg-late-consume',
        queue_name='q-late-consume')

    c.consume(on_message)
    c.close()

# TODO make a test which sends messages to a channel and consume another (but the same queue)
# 2021-05-31 11:48:34 [CRITICAL] Received METHOD frame for unregistered channel 6 on <SelectConnection OPEN transport=<pika.adapters.utils.io_services_utils._AsyncPlaintextTransport object at 0x1107c41f0> params=<ConnectionParameters host=localhost port=5672 virtual_host=/ ssl=False>> (connection.py:1592)
