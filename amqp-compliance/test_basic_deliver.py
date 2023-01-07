import logging
import threading

import helper

def test_basic_deliver_redelivered():
    """
    Test redelivered flag.
    """
    log = logging.getLogger()

    def consume_without_ack(channel, condition):
        counter = 0

        for method, properties, body in channel.consume("q-redelivered"):
            counter += 1

            if counter == 2:
                break

        channel.cancel()
        with condition:
            condition.notify()

    def consume_with_ack(channel, condition):
        counter = 0

        for method, properties, body in channel.consume("q-redelivered"):
            assert method.redelivered

            counter += 1

            if counter == 2:
                break

        channel.cancel()
        with condition:
            condition.notify()

    with helper.channel(3) as producer:
        with helper.direct_exchange(producer, "x-redelivered", ("q-redelivered", "rk")):
            for i in range(2):
                producer.basic_publish("x-redelivered", "rk", "Deliver twice")

            log.info("Finished publishing")

            with helper.channel(2) as consumer:
                consuming_without_acking = threading.Condition()
                threading.Thread(target=consume_without_ack, args=(consumer, consuming_without_acking)).start()

                with consuming_without_acking:
                    consuming_without_acking.wait()

            with helper.channel(4) as acker:
                consuming_with_acking = threading.Condition()
                threading.Thread(target=consume_with_ack, args=(acker, consuming_with_acking)).start()

                with consuming_with_acking:
                    consuming_with_acking.wait()
