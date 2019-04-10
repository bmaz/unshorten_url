from redis import Redis
from rq import Queue, Worker

redis = Redis(host='redis', port=6379)
queue = Queue('inputs', connection=redis)

worker = Worker([queue], connection=redis)
worker.work()
