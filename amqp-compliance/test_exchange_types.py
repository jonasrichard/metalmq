import helper
import logging
import pika
import pytest
import threading

TIMEOUT = 1

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

    def start(self):
        th = threading.Thread(target=self.consume)
        th.start()

        return th

def test_direct_exchange_two_queues(caplog):
    """
    Direct exchange with two queues, routing keys are queue names.
    """
    caplog.set_level(logging.INFO)

    with helper.channel(8) as channel:
        with helper.direct_exchange(channel, "2-exchange", "1-queue", "2-queue"):
            consumer1 = QueueConsumer("1-queue", 5)
            consumer1_thread = consumer1.start()

            consumer2 = QueueConsumer("2-queue", 5)
            consumer2_thread = threading.Thread(target=consumer2.consume, args=())
            consumer2_thread.start()

            for i in range(0, 10):
                channel.basic_publish(
                        "2-exchange",
                        f"{i % 2 + 1:d}-queue",
                        f"Message {i:d}",
                        properties=pika.BasicProperties(message_id=f"id{i:d}"))

            consumer1_thread.join(timeout=TIMEOUT)
            consumer2_thread.join(timeout=TIMEOUT)

    assert len(consumer1.messages) == 5
    assert len(consumer2.messages) == 5

def test_direct_exchange_multi_queues(caplog):
    """
    Direct exchange with two queues, same routing key, sending the messages
    to both queues.
    """
    caplog.set_level(logging.INFO)

    with helper.channel(9) as channel:
        with helper.direct_exchange(channel, "multi-exchange", ("multi-1-queue", "same"), ("multi-2-queue", "same")):
            consumer1 = QueueConsumer("multi-1-queue", 5)
            consumer1_thread = consumer1.start()

            consumer2 = QueueConsumer("multi-2-queue", 5)
            consumer2_thread = consumer2.start()

            for i in range(0, 5):
                channel.basic_publish(
                        "multi-exchange",
                        "same",
                        f"Message {i:d}",
                        properties=pika.BasicProperties(message_id=f"id{i:d}"))

            consumer1_thread.join(timeout=TIMEOUT)
            consumer2_thread.join(timeout=TIMEOUT)

    assert len(consumer1.messages) == 5, "multi-1-queue consumer hasn't got enough messages"
    assert len(consumer2.messages) == 5, "multi-2-queue consumer hasn't got enough messages"

def test_fanout_exchange_two_queues(caplog):
    """
    Fanout exchange broadcast messages to two queues.
    """
    caplog.set_level(logging.INFO)

    with helper.channel(10) as channel:
        with helper.fanout_exchange(channel, "fanout-exchange", "fan-1-queue", "fan-2-queue"):
            consumer1 = QueueConsumer("fan-1-queue", 10)
            consumer1_thread = threading.Thread(target=consumer1.consume, args=())
            consumer1_thread.start()

            consumer2 = QueueConsumer("fan-2-queue", 10)
            consumer2_thread = threading.Thread(target=consumer2.consume, args=())
            consumer2_thread.start()

            for i in range(0, 10):
                channel.basic_publish(
                        "fanout-exchange",
                        f"fan-{i % 2 + 1:d}-queue",
                        f"Message {i:d}",
                        properties=pika.BasicProperties(message_id=f"id{i:d}"))

            consumer1_thread.join(timeout=TIMEOUT)
            consumer2_thread.join(timeout=TIMEOUT)

    assert len(consumer1.messages) == 10
    assert len(consumer2.messages) == 10

def test_topic_exchange():
    with helper.channel(11) as channel:
        def send(market, ticker, price):
            routing_key = f"stocks.{market:s}.{ticker:s}"

            channel.basic_publish(
                    "prices",
                    routing_key,
                    f"Price {price:f}",
                    properties=pika.BasicProperties(message_id=ticker))

        with helper.topic_exchange(channel, "prices", ("nwse", "stocks.nwse.*"), ("dax", "stocks.dax.*"), ("all", "stocks.#")):
            all = QueueConsumer("all", 5)
            all_thread = all.start()

            dax = QueueConsumer("dax", 2)
            dax_thread = dax.start()

            nwse = QueueConsumer("nwse", 3)
            nwse_thread = nwse.start()

            send("nwse", "goog", 78.2)
            send("dax", "dai", 69.2)
            send("dax", "bmw", 91.3)
            send("nwse", "tsla", 1099.0)
            send("nwse", "orcl", 89.1)

            all_thread.join(timeout=TIMEOUT)
            dax_thread.join(timeout=TIMEOUT)
            nwse_thread.join(timeout=TIMEOUT)

    assert len(all.messages) == 5
    assert len(dax.messages) == 2
    assert len(nwse.messages) == 3

def test_header_exchange():
    client = helper.connect()
    channel = client.channel()

    channel.exchange_declare(exchange="content", exchange_type="headers")
    channel.queue_declare("text")
    channel.queue_bind("text", "content", arguments={"header-type": "text"})
    channel.queue_declare("xml")
    channel.queue_bind("xml", "content", arguments={"header-type": "xml"})

    def send(content_type):
        channel.basic_publish(
                "content",
                "",
                body=f"Content is {content_type:s}",
                properties=pika.BasicProperties(
                    message_id=content_type,
                    headers={"header-type": content_type}))

    text = QueueConsumer("text", 1)
    text_thread = text.start()

    xml = QueueConsumer("xml", 1)
    xml_thread = xml.start()

    send("text")
    send("xml")
    send("other")

    text_thread.join(timeout=TIMEOUT)
    xml_thread.join(timeout=TIMEOUT)

    channel.queue_unbind("text", "content", arguments={"header-type": "text"})
    channel.queue_unbind("xml", "content", arguments={"header-type": "xml"})

    channel.queue_delete("text")
    channel.queue_delete("xml")

    channel.exchange_delete("content")

    channel.close()
    client.close()

    assert 1 == len(text.messages)
    assert 1 == len(xml.messages)
