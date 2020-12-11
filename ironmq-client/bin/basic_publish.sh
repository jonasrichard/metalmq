#!/usr/bin/env python

import pika
import sys

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='test', exchange_type='fanout')

# channel.queue_declare(queue='logs-queue')
# channel.queue_bind(queue='logs-queue', exchange='logs')

message = ' '.join(sys.argv[1:]) or "info: Hello World!"
channel.basic_publish(exchange='test', routing_key='', body=message)

print(" [x] Sent %r" % message)

connection.close()
