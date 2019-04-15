from redis import Redis
from rq import Queue, Worker
import requests
from requests.adapters import TimeoutSauce
from urllib3.exceptions import ReadTimeoutError
from ssl import CertificateError
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

class MyTimeout(TimeoutSauce):
    def __init__(self, *args, **kwargs):
        if kwargs['connect'] is None:
            kwargs['connect'] = 10
        if kwargs['read'] is None:
            kwargs['read'] = 10
        super(MyTimeout, self).__init__(*args, **kwargs)

requests.adapters.TimeoutSauce = MyTimeout

redis = Redis(host='redis', port=6379)
queue = Queue('inputs', connection=redis, default_timeout=12)

worker = Worker([queue], connection=redis)
worker.work()
