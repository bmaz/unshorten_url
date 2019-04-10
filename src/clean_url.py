from requests import Session, exceptions
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from redis import Redis
from rq import Queue
import json
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
            full_url = urlunparse(parse)
            parse = parse._replace(query="", params="", scheme="http", fragment="")
            clean_url = urlunparse(parse)
            return {"domain": parse.netloc, "full_url": full_url, "standard_url": clean_url, "short_url": short_url}
        return {"error": parse.path, "short_url": short_url}


def write_output(data):
    with open("/data/"+outputfile, "a+", encoding="utf-8") as f:
        json.dump(data, f)
        f.write("\n")


def job(req):
    parse = return_urlparse(req["url"])
    parse["id"] = req["id"]
    queue_out.enqueue(write_output, parse)


