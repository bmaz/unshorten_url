import pika
from conf import rabbit_host, rabbit_port, rabbit_user, rabbit_password
import json
import logging

class RabbitQueue:
    def __init__(self, name):
        self.rabbit_client = self.open_rabbit_connection()
        self.name = name
        self.queue = self.open_rabbit_channel()

    def open_rabbit_connection(self):
        for i in range(3):
            try:
                rabbit_client = pika.BlockingConnection(
                    pika.ConnectionParameters(host=rabbit_host, port=rabbit_port, heartbeat=600,
                                              blocked_connection_timeout=300,
                                              credentials=pika.credentials.PlainCredentials(
                                                  username=rabbit_user,
                                                  password=rabbit_password)))
                break
            except pika.exceptions.AMQPConnectionError:
                logging.error("pika AMQPConnectionError, retrying")
            except Exception as error:
                logging.error("other error, retrying " + str(error))
        return rabbit_client

    def open_rabbit_channel(self):
        queue = self.rabbit_client.channel()
        queue.queue_declare(queue=self.name)
        return queue

    def publish(self, data):
        try:
            self.queue.basic_publish(exchange='',
                                           routing_key=self.name,
                                           body=json.dumps(data))
        except pika.exceptions.AMQPConnectionError:
            logging.error("AMQPConnectionError, trying to reconnect")
            self.rabbit_client = self.open_rabbit_connection()
            self.queue = self.open_rabbit_channel()
            self.publish(data)