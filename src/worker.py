from redis import Redis
from rq import Queue, Worker


redis = Redis(host='redis', port=6379)
queue = Queue('inputs', connection=redis)

# Start a worker with a custom name
worker = Worker([queue], connection=redis, name='worker')
worker.work()
