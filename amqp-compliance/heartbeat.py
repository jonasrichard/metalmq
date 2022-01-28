import helper
import pika
import time

def connect_and_wait():
    with helper.channel(1) as channel:
        time.sleep(120)

if __name__ == "__main__":
    connect_and_wait()
