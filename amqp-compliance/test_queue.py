import helper
import logging
import pika
import pytest
import threading
import time

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

        def on_return(ch, method, props, body):
            global returned
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
    with helper.channel(1) as channel:
        channel.queue_declare("exclusive-queue", exclusive=True)

    with helper.channel(2) as channel:
        channel.exchange_declare("exclusive-try-bind")

        with pytest.raises(pika.exceptions.ChannelError) as exp:
            assert 403 == exp.value.reply_code
