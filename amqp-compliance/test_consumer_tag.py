import datetime
import helper
import logging
import pika

LOG = logging.getLogger()

class PublishConsumeTest():
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
        self.channel.exchange_declare(exchange=self.exchange_name)
        self.channel.queue_declare(self.queue_name)
        self.channel.queue_bind(self.queue_name, self.exchange_name)

    def close(self, without_cleanup=False):
        if not without_cleanup:
            self.channel.queue_unbind(self.queue_name, self.exchange_name)
            self.channel.queue_delete(self.queue_name)
            self.channel.exchange_delete(self.exchange_name)

        self.channel.close()
        self.connection.close()

    def publish(self, body):
        self.channel.basic_publish(
                self.exchange_name,
                self.queue_name,
                body,
                pika.BasicProperties(content_type='text/plain', delivery_mode=1))

    def consume(self, on_message_delivered):
        self.channel.basic_consume(
                self.queue_name,
                on_message_callback=lambda ch, method, props, body: on_message_delivered(self, ch, method, props, body))
        self.channel.start_consuming()

    def cancel_consume(self):
        self.channel.stop_consuming()

def on_msg(obj, message_body, expected_body):
    LOG.info("on_msg %s", message_body)
    assert message_body.decode("utf-8") == expected_body
    obj.cancel_consume()

def test_xyz(caplog):
    body = datetime.datetime.now().strftime("%H:%M:%S.%f")

    t = PublishConsumeTest(channel_number=3, exchange_name='ctag-exchange', queue_name='ctag_queue')
    t.declare_exchange()
    t.publish(body)
    t.consume(lambda obj, ch, method, props, msg_body: on_msg(obj, msg_body, body))
    t.close(without_cleanup=True)

current_consumer_tag = ""
messages_received = 0
last_delivery_tag = -1

def on_receive(channel, method, properties, body):
    global messages_received
    global last_delivery_tag

    LOG.info('Got message %s %s %s', method, properties, body)
    messages_received += 1

    assert method.consumer_tag == current_consumer_tag

    assert last_delivery_tag < method.delivery_tag
    last_delivery_tag = method.delivery_tag

    #assert not (method.redelivered)

    assert method.routing_key == 'ctag-queue'

    channel.basic_ack(delivery_tag=method.delivery_tag, multiple=False)

    if messages_received == 10:
        channel.stop_consuming()


def test_server_generated_consumer_tags_one_by_one_ack(caplog):
    global current_consumer_tag
    exchange_name = 'ctag-exchange'
    queue_name = 'ctag-queue'

    conn = helper.connect()
    channel = declare_exchange(connection=conn, channel=7, exchange_name=exchange_name, queue_name=queue_name)

    for i in range(0, 10):
        channel.basic_publish(
                exchange_name,
                queue_name,
                'Message {}'.format(i),
                pika.BasicProperties(content_type='text/plain', delivery_mode=1))

    current_consumer_tag = channel.basic_consume(queue_name, on_message_callback=on_receive)
    channel.start_consuming()

    cleanup(channel, exchange_name, queue_name)
    conn.close()

def on_late_receive(channel, method, properties, body):
    LOG.info("Late message %s %s %s", method, properties, body)

    channel.basic_ack(delivery_tag=method.delivery_tag, multiple=False)

def test_publish_and_then_consume(caplog):
    conn = helper.connect()
    channel = declare_exchange(connection=conn, channel=6, exchange_name='xchg-late-consume', queue_name='q-late-consume')

    for i in range(0, 10):
        channel.basic_publish(
                'xchg-late-consume',
                'q-late-consume',
                'Message {}'.format(i),
                pika.BasicProperties(content_type='text/plain', delivery_mode=1))

    conn.close()

    consumer = helper.connect()
    channel = consumer.channel(channel_number=6)

    channel.basic_consume('q-late-consume', on_message_callback=on_late_receive)
    channel.start_consuming()

    cleanup(channel, 'xchg-late-consume', 'q-late-consume')
    consumer.close()

# TODO make a test which sends messages to a channel and consume another (but the same queue)
# 2021-05-31 11:48:34 [CRITICAL] Received METHOD frame for unregistered channel 6 on <SelectConnection OPEN transport=<pika.adapters.utils.io_services_utils._AsyncPlaintextTransport object at 0x1107c41f0> params=<ConnectionParameters host=localhost port=5672 virtual_host=/ ssl=False>> (connection.py:1592)
