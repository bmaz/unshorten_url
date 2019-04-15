import requests
from requests.adapters import TimeoutSauce
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from redis import Redis
from rq import Queue
import csv
import os
import logging
from conf import outputfile, proxies, nb_workers, date_format
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch, helpers
import time

logging.basicConfig(format='%(asctime)s - %(levelname)s : %(message)s', level=logging.INFO)

s = requests.Session()
s.trust_env = False
s.proxies = proxies
try:
    cache = Redis(host='redis', port=6379, decode_responses=True)
except Exception:
    time.sleep(30)
queue_out = Queue('outputs', connection=cache)
queue_in = Queue('inputs', connection=cache, default_timeout=12)

class MyTimeout(TimeoutSauce):
    def __init__(self, *args, **kwargs):
        if kwargs['connect'] is None:
            kwargs['connect'] = 10
        if kwargs['read'] is None:
            kwargs['read'] = 10
        super(MyTimeout, self).__init__(*args, **kwargs)

requests.adapters.TimeoutSauce = MyTimeout

def build_query(start):
    query = {
        "_source": ["entities.urls", "created_at"],
        "query": {
            "bool": {
                "filter": [
                    {"exists": {"field": "entities.urls"}},
                    {"range": {"created_at": {
                        "gte": start.strftime(date_format),
                        "lte": (start + timedelta(hours=1)).strftime(date_format),
                    }}},
                ]
            }
        }
    }
    return query

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
    while True:
        with open("/data/"+outputfile, "a+", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers, delimiter=";", quoting=csv.QUOTE_ALL)
            writer.writerow(data)


def job(req):
    parse = return_urlparse(req["url"])
    parse["id"] = req["id"]
    parse["created_at"] = req["created_at"]
    queue_out.enqueue(write_output, parse)

if __name__ == "__main__":
    start = datetime(2018, 12, 1, 8, 0, 0)
    end = datetime(2018, 12, 31, 23, 59, 59)

    es = Elasticsearch("otmedia-srv01.priv.ina:9292")

    while True:
        query = build_query(start)
        logging.info(query["query"]["bool"]["filter"])
        scan = helpers.scan(client=es, index="otmtweets-2018-12*", query=query)
        counter = 0
        for doc in scan:
            counter += 1
            for url in doc["_source"]["entities"]["urls"]:
                if not url["expanded_url"].startswith("https://twitter.com/"):
                    req = {"id": doc["_id"], "created_at": doc["_source"]["created_at"], "url":url["expanded_url"]}
                    queue_in.enqueue(job, req)
                    # r = s.post("http://server:5000/", data={"id": doc["_id"], "url":url["expanded_url"]})
        start = start + timedelta(hours=1)
        if start > end:
            break
    logging.info("DONE")
