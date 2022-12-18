"""Test queue related behaviours"""
import logging
import threading

import pytest
import pika
import helper

LOG = logging.getLogger()

def test_queue_delete_unbinds_exchange():
    """
    Test if deleting a queue will unbind from the exchange. During the test we
    send an immediate message, which should be returned in a basic.return.
    """
    returned = False

    with helper.channel(1) as channel:
        channel.exchange_declare("silent-unbind-exchange")
        channel.queue_declare("silent-unbind-queue")
        channel.queue_bind("silent-unbind-queue", "silent-unbind-exchange", "routing-key")

        channel.queue_delete("silent-unbind-queue")

        def on_return(_ch, method, props, body):
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

def test_exclusive_queue():
    """
    An exclusive queue can be bound, consumed, be unbound and deleted by the same
    connection which crated that.
    """
    def declaring_connection(can_bind, end_condition):
        """
        Keeping the declaring connection open in order that the exclusive queue
        remain alive.

        :param threading.Condition:
            Condition to say that the other connection start binding.
        :param threading.Condition:
            This condition is notified when the outer test completes.
        """
        with helper.channel(1) as channel:
            channel.queue_declare("exclusive-queue", exclusive=True)

            with can_bind:
                can_bind.notify()

            with end_condition:
                end_condition.wait()

    can_bind = threading.Condition()
    end_condition = threading.Condition()

    try:
        threading.Thread(target=declaring_connection, args=(can_bind, end_condition,)).start()

        with helper.channel(2) as channel:
            channel.exchange_declare("exclusive-try-bind")

            with can_bind:
                can_bind.wait()

            #with pytest.raises(pika.exceptions.ChannelError) as exp:
            with pytest.raises(pika.exceptions.ChannelWrongStateError) as exp:
                channel.queue_bind("exclusive-queue", "exclusive-try-bind", "routing")

            #assert 403 == exp.value.reply_code
    finally:
        with end_condition:
            end_condition.notify()
