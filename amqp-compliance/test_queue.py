"Analyze queue functionality under test conditions"

import logging
import time

import pytest
import pika
import pika.channel
import pika.exceptions
import helper

LOG = logging.getLogger()

def test_queue_delete_unbinds_exchange():
    """
    Check whether removing a queue results in disassociation from the exchange. In this test, an
    instant message is transmitted, which should be replied with a basic.return.
    """
    returned = False

    with helper.channel(1) as channel:
        channel.exchange_declare("silent-unbind-exchange")
        channel.queue_declare("silent-unbind-queue")
        channel.queue_bind("silent-unbind-queue", "silent-unbind-exchange", "routing-key")

        channel.queue_delete("silent-unbind-queue")

        def on_return(
                ch: pika.channel.Channel,
                method: pika.spec.Basic.Return,
                props: pika.spec.BasicProperties,
                body: bytes):
            nonlocal returned
            LOG.info("Return %s %s %s", method, props, body)
            returned = True

        channel.add_on_return_callback(on_return)
        channel.confirm_delivery()

        with pytest.raises(pika.exceptions.UnroutableError) as exp:
            channel.basic_publish(
                    "silent-unbind-exchange",
                    "routing-key",
                    "Should be unrouted",
                    mandatory=True)

        assert 1 == len(exp.value.messages)

        msg = exp.value.messages[0]
        assert 312 == msg.method.reply_code
        assert "NO_ROUTE" == msg.method.reply_text
        assert "routing-key" == msg.method.routing_key
        assert "silent-unbind-exchange" == msg.method.exchange

def test_exclusive_queue_cannot_be_bound_by_other_connection():
    """
    A single connection can establish, consume, release, and delete an exclusive queue it initially created.
    """
    with helper.channel(1) as declaring_channel:
        declaring_channel.queue_declare("exclusive-queue", exclusive=True)

        with helper.channel(1) as offending_channel:
            offending_channel.exchange_declare("exclusive-try-bind")

            with pytest.raises(pika.exceptions.ChannelClosedByBroker) as exp:
                offending_channel.queue_bind("exclusive-queue", "exclusive-try-bind", "routing")

            assert 405 == exp.value.reply_code

def test_exclusive_queue_cannot_consume_by_other_connection():
    """
    A unique queue can't be consumed simultaneously by another connection.
    """
    def on_message(_ch, _method, _properties, _body):
        pass

    with helper.channel(1) as declaring_channel:
        declaring_channel.queue_declare("exclusive-queue-2", exclusive=True)

        with helper.channel(1) as consuming_channel:
            with pytest.raises(pika.exceptions.ChannelClosedByBroker) as exp:
                consuming_channel.basic_consume("exclusive-queue-2", on_message)

            assert 405 == exp.value.reply_code

def test_queue_declare_without_name_has_generated_name():
    """
    In instances where the client fails to specify a name upon queue creation, the server should automatically assign one instead.
    """
    with helper.channel(3) as declaring_channel:
        declare_ok = declaring_channel.queue_declare("")

        assert '' != declare_ok.method.queue

def test_queue_declare_should_give_back_message_count_if_queue_exists():
    """
    The Declare.QueueOk server provides a message count and consumer count when a queue that already exists is declared.
    """
    with helper.channel(4) as declaring_channel:
        declaring_channel.queue_declare("q-msg-count")
        declaring_channel.queue_purge("q-msg-count")
        declaring_channel.exchange_declare("x-msg-count")
        declaring_channel.queue_bind("q-msg-count", "x-msg-count", "q-msg-count")

        for i in range(0, 5):
            declaring_channel.basic_publish(
                    "x-msg-count",
                    "q-msg-count",
                    f"Body {i}",
                    mandatory=True)

        time.sleep(0.5)

        declare_ok = declaring_channel.queue_declare("q-msg-count", passive=True)

        assert declare_ok.method.message_count == 5
