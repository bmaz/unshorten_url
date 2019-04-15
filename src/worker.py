from redis import Redis
from rq import Queue, Worker

redis = Redis(host='redis', port=6379)
queue = Queue('inputs', connection=redis, default_timeout=12)

worker = Worker([queue], connection=redis)
worker.work()
