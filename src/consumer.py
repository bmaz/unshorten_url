from redis import Redis
from rq import Queue, Worker


redis = Redis(host='redis', port=6379)
queue_out = Queue('outputs', connection=redis)

# Start a worker with a custom name
worker = Worker([queue_out], connection=redis, name='consumer')
worker.work()
