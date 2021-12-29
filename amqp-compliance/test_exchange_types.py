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

    client = helper.connect()
    channel = client.channel(channel_number=8)

    channel.exchange_declare(exchange="2-exchange", exchange_type="direct")
    channel.queue_declare("1-queue")
    channel.queue_bind("1-queue", "2-exchange", "1-queue")
    channel.queue_declare("2-queue")
    channel.queue_bind("2-queue", "2-exchange", "2-queue")

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

    channel.queue_unbind("1-queue", "2-exchange", "1-queue")
    channel.queue_unbind("2-queue", "2-exchange", "2-queue")
    channel.exchange_delete("2-exchange")
    channel.queue_delete("1-queue")
    channel.queue_delete("2-queue")

    channel.close()
    client.close()

    assert 5 == len(consumer1.messages)
    assert 5 == len(consumer2.messages)

def test_direct_exchange_multi_queues(caplog):
    """
    Direct exchange with two queues, same routing key, sending the messages
    to both queues.
    """
    caplog.set_level(logging.INFO)

    client = helper.connect()
    channel = client.channel(channel_number=8)

    channel.exchange_declare(exchange="multi-exchange", exchange_type="direct")
    channel.queue_declare("multi-1-queue")
    channel.queue_bind("multi-1-queue", "multi-exchange", "same")
    channel.queue_declare("multi-2-queue")
    channel.queue_bind("multi-2-queue", "multi-exchange", "same")

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

    channel.queue_unbind("multi-1-queue", "multi-exchange", "same")
    channel.queue_unbind("multi-2-queue", "multi-exchange", "same")
    channel.exchange_delete("multi-exchange")
    channel.queue_delete("multi-1-queue")
    channel.queue_delete("multi-2-queue")

    channel.close()
    client.close()

    assert 5 == len(consumer1.messages), "multi-1-queue consumer hasn't got enough messages"
    assert 5 == len(consumer2.messages), "multi-2-queue consumer hasn't got enough messages"

def test_fanout_exchange_two_queues(caplog):
    """
    Fanout exchange broadcast messages to two queues.
    """
    caplog.set_level(logging.INFO)

    client = helper.connect()
    channel = client.channel(channel_number=8)

    channel.exchange_declare(exchange="fanout-exchange", exchange_type="fanout")
    channel.queue_declare("fan-1-queue")
    channel.queue_bind("fan-1-queue", "fanout-exchange", "fan-1-queue")
    channel.queue_declare("fan-2-queue")
    channel.queue_bind("fan-2-queue", "fanout-exchange", "fan-2-queue")

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

    channel.queue_unbind("fan-1-queue", "fanout-exchange", "fan-1-queue")
    channel.queue_unbind("fan-2-queue", "fanout-exchange", "fan-2-queue")
    channel.exchange_delete("fanout-exchange")
    channel.queue_delete("fan-1-queue")
    channel.queue_delete("fan-2-queue")

    channel.close()
    client.close()

    assert 10 == len(consumer1.messages)
    assert 10 == len(consumer2.messages)

def test_topic_exchange():
    client = helper.connect()
    channel = client.channel()

    channel.exchange_declare(exchange="prices", exchange_type="topic")
    channel.queue_declare("nwse")
    channel.queue_bind("nwse", "prices", "stocks.nwse.*")
    channel.queue_declare("dax")
    channel.queue_bind("dax", "prices", "stocks.dax.*")
    channel.queue_declare("all")
    channel.queue_bind("all", "prices", "stocks.#")

    def send(market, ticker, price):
        routing_key = f"stocks.{market:s}.{ticker:s}"

        channel.basic_publish(
                "prices",
                routing_key,
                f"Price {price:f}",
                properties=pika.BasicProperties(message_id=ticker))

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

    channel.queue_unbind("nwse", "prices", "stocks.nwse.*")
    channel.queue_unbind("dax", "prices", "stocks.dax.*")
    channel.queue_unbind("all", "prices", "stocks.#")

    channel.queue_delete("nwse")
    channel.queue_delete("dax")
    channel.queue_delete("all")

    channel.exchange_delete("prices")

    channel.close()
    client.close()

    assert 5 == len(all.messages)
    assert 2 == len(dax.messages)
    assert 3 == len(nwse.messages)

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
