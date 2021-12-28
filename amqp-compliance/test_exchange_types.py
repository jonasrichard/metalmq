import helper
import logging
import pika
import pytest
import threading

class QueueConsumer():
    """
    Consume the queue and store the message_number messages.
    """
    def __init__(self, queue_name, message_number):
        self.connection = helper.connect()
        self.channel = self.connection.channel()

        self.message_number = message_number
        self.messages = {}
        self.channel.basic_consume(queue_name, on_message_callback=self.__on_message)

    def __on_message(self, channel, method, props, body):
        self.messages[props.message_id] = body
        channel.basic_ack(method.delivery_tag)
        if len(self.messages) == self.message_number:
            channel.stop_consuming()

    def consume(self):
        self.channel.start_consuming()

        self.channel.close()
        self.connection.close()

def test_direct_exchange_two_queues(caplog):
    caplog.set_level(logging.INFO)

    client = helper.connect()
    channel = client.channel(channel_number=8)

    channel.exchange_declare(exchange="2-exchange", exchange_type="direct")
    channel.queue_declare("1-queue")
    channel.queue_bind("1-queue", "2-exchange", "1-queue")
    channel.queue_declare("2-queue")
    channel.queue_bind("2-queue", "2-exchange", "2-queue")

    consumer1 = QueueConsumer("1-queue", 5)
    consumer1_thread = threading.Thread(target=consumer1.consume, args=())
    consumer1_thread.start()

    consumer2 = QueueConsumer("2-queue", 5)
    consumer2_thread = threading.Thread(target=consumer2.consume, args=())
    consumer2_thread.start()

    for i in range(0, 10):
        channel.basic_publish(
                "2-exchange",
                f"{i % 2 + 1:d}-queue",
                f"Message {i:d}",
                properties=pika.BasicProperties(message_id=f"id{i:d}"))

    consumer1_thread.join()
    consumer2_thread.join()

    channel.queue_unbind("1-queue", "2-exchange", "1-queue")
    channel.queue_unbind("2-queue", "2-exchange", "2-queue")
    channel.exchange_delete("2-exchange")
    channel.queue_delete("1-queue")
    channel.queue_delete("2-queue")

    channel.close()
    client.close()

    assert 5 == len(consumer1.messages)
    assert 5 == len(consumer2.messages)
