from rabbit_queue import RabbitQueue
import logging
import json
from clean_url import job

logging.basicConfig(format='%(asctime)s - %(levelname)s : %(message)s', level=logging.INFO)


def on_message(channel, method_frame, header_frame, body):
    job(json.loads(body.decode("utf-8")))
    channel.basic_ack(delivery_tag=method_frame.delivery_tag)


if __name__ == "__main__":
    queue_in = RabbitQueue("inputs")
    queue_in.queue.basic_consume(on_message, queue_in.name)
    queue_in.queue.start_consuming()