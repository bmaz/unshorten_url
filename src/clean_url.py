from requests import Session, exceptions
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from redis import Redis
from rq import Queue
import csv
import os
import logging
from conf import outputfile, proxies

logging.basicConfig(format='%(asctime)s - %(levelname)s : %(message)s', level=logging.INFO)

s = Session()
s.trust_env = False
s.proxies = proxies
cache = Redis(host='redis', port=6379, decode_responses=True)
queue_out = Queue('outputs', connection=cache)


def expand_url(url):
    try:
        res = s.get(url, timeout=10)
        cache.set(url, res.url)
        return res.url
    except exceptions.ProxyError:
        return "Error/Proxy"


def return_urlparse(short_url):
    if short_url:
        long_url = cache.get(short_url)
        if long_url is None:
            long_url = expand_url(short_url)
        parse = urlparse(long_url)
        if parse.netloc != "Error":
            # full_url = urlunparse(parse)
            # parse = parse._replace(params="", scheme="http", fragment="")
            # clean_url = urlunparse(parse)
            return {"short_url": short_url, "domain": parse.netloc, "full_url": long_url}
        return {"short_url": short_url, "error": parse.path}


def write_output(data):
    headers = ["id", "short_url", "domain", "full_url", "error"]
    file_exists = os.path.isfile("/data/"+outputfile)
    with open("/data/"+outputfile, "a+", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers, delimiter=";", quoting=csv.QUOTE_ALL)
        if not file_exists:
            writer.writeheader()
        writer.writerow(data)


def job(req):
    parse = return_urlparse(req["url"])
    parse["id"] = req["id"]
    queue_out.enqueue(write_output, parse)


