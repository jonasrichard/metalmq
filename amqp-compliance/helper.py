__name__ = "helper"

import pika

def connect(username="guest", password="guest", host="metalmq", port=5672, vhost="/"):
    return pika.BlockingConnection(
            pika.ConnectionParameters(
                host=host,
                port=port,
                virtual_host=vhost,
                credentials=pika.PlainCredentials(username, password)
                )
            )
