import helper
import logging
import pika
import pytest
import threading
import time

LOG = logging.getLogger()

def test_queue_delete_unbinds_exchange():
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

        breakpoint()
        with pytest.raises(pika.exceptions.UnroutableError) as exp:
            channel.basic_publish(
                    "silent-unbind-exchange",
                    "routing-key",
                    "Should be unrouted",
                    mandatory=True)

        assert exp.messages.len() == 1
