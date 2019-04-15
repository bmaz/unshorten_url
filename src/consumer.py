from redis import Redis
from rq import Queue, Worker
import csv
import os

redis = Redis(host='redis', port=6379)
queue_out = Queue('outputs', connection=redis)

worker = Worker([queue_out], connection=redis)
worker.work()
