import time

import helper

def test_basic_get():
    """
    Test Basic.Get in a simple case.
    """
    with helper.channel(2) as sender:
        sender.exchange_declare("x-get-test")
        sender.queue_declare("q-get-test")
        sender.queue_bind("q-get-test", "x-get-test", "rk")
        sender.queue_purge("q-get-test")

        with helper.channel(7) as consumer:
            (method, _, _) = consumer.basic_get("q-get-test")

            assert method is None

            sender.basic_publish("x-get-test", "rk", "A message")
            sender.basic_publish("x-get-test", "rk", "Another message")
            time.sleep(0.2)

            (method, properties, _message_body) = consumer.basic_get("q-get-test")

            assert method.exchange == "x-get-test"
            assert method.routing_key == "rk"
            assert method.message_count == 1

            consumer.basic_ack(method.delivery_tag)

            consumer.basic_get("q-get-test")

# Test redelivered is True
