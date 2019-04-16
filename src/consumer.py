from rabbit_queue import RabbitQueue
import logging
import json
from conf import outputfile
import csv
import os

logging.basicConfig(format='%(asctime)s - %(levelname)s : %(message)s', level=logging.INFO)
headers = ["id", "created_at", "short_url", "domain", "full_url", "error"]

def write_headers():
    if not os.path.isfile("/data/"+outputfile):
        with open("/data/"+outputfile, "a+", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers, delimiter=";", quoting=csv.QUOTE_ALL)
            writer.writeheader()

def on_message(channel, method_frame, header_frame, body):
    writer.writerow(json.loads(body.decode("utf-8")))
    channel.basic_ack(delivery_tag=method_frame.delivery_tag)

if __name__ == "__main__":
    queue_out = RabbitQueue("outputs")
    with open("/data/" + outputfile, "a+", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers, delimiter=";", quoting=csv.QUOTE_ALL)
        queue_out.queue.basic_consume(on_message, queue_out.name)
        queue_out.queue.start_consuming()