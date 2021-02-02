#!/usr/bin/env python

import pika
import sys

def callback(ch, method, properties, body):
    print("  [x] %r" % body)

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()


channel.basic_consume(queue='queue-test', on_message_callback=callback, auto_ack=True)

channel.start_consuming()
