import requests
from requests.adapters import TimeoutSauce
from urllib3.exceptions import ReadTimeoutError
from ssl import CertificateError
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from redis import Redis
from rq import Queue
import csv
import os
import logging
from conf import outputfile, proxies, nb_workers, date_format
import time

logging.basicConfig(format='%(asctime)s - %(levelname)s : %(message)s', level=logging.INFO)

s = requests.Session()
s.trust_env = False
s.proxies = proxies
adapter = requests.adapters.HTTPAdapter(pool_connections=nb_workers, pool_maxsize=nb_workers)
s.mount('http://', adapter)
try:
    cache = Redis(host='redis', port=6379, decode_responses=True)
except Exception:
    time.sleep(30)
    cache = Redis(host='redis', port=6379, decode_responses=True)
queue_out = Queue('outputs', connection=cache)

class MyTimeout(TimeoutSauce):
    def __init__(self, *args, **kwargs):
        if kwargs['connect'] is None:
            kwargs['connect'] = 10
        if kwargs['read'] is None:
            kwargs['read'] = 10
        super(MyTimeout, self).__init__(*args, **kwargs)

requests.adapters.TimeoutSauce = MyTimeout



def expand_url(url):
    try:
        res = s.get(url, timeout=10)
        cache.set(url, res.url)
        return res.url
    except requests.exceptions.ProxyError:
        cache.set(url, "Error/ProxyError")
        return "Error/ProxyError"
    except requests.exceptions.SSLError:
        cache.set(url, "Error/SSLError")
        return "Error/SSLError"
    except ReadTimeoutError:
        cache.set(url, "Error/ReadTimeoutError")
        return "Error/ReadTimeoutError"
    except requests.exceptions.ContentDecodingError:
        cache.set(url, "Error/ContentDecodingError")
        return "Error/ContentDecodingError"
    except requests.exceptions.Timeout:
        cache.set(url, "Error/TimeoutError")
        return "Error/TimeoutError"
    except CertificateError:
        cache.set(url, "Error/CertificateError")
        return "Error/CertificateError"
    except UnicodeDecodeError:
        cache.set(url, "Error/UnicodeDecodeError")
        return "Error/UnicodeDecodeError"
    except requests.exceptions.TooManyRedirects:
        cache.set(url, "Error/TooManyRedirects")
        return "Error/TooManyRedirects"
    except Exception as error:
        logging.error(error)
        return "Error/Other"


def return_urlparse(short_url):
    if short_url:
        long_url = cache.get(short_url)
        if long_url is None:
            long_url = expand_url(short_url)
        parse = urlparse(long_url)
        if long_url.startswith("Error"):
            return {"short_url": short_url, "error": long_url}
        return {"short_url": short_url, "domain": parse.netloc, "full_url": long_url}


def write_output(data):
    headers = ["id", "created_at", "short_url", "domain", "full_url", "error"]
    if not os.path.isfile("/data/"+outputfile):
        with open("/data/"+outputfile, "a+", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers, delimiter=";", quoting=csv.QUOTE_ALL)
            writer.writeheader()

    with open("/data/"+outputfile, "a+", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers, delimiter=";", quoting=csv.QUOTE_ALL)
        writer.writerow(data)

def timeout_handler():
    pass

def job(req):
    parse = return_urlparse(req["url"])
    parse["id"] = req["id"]
    parse["created_at"] = req["created_at"]
    queue_out.enqueue(write_output, parse)
