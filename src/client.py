import requests
import logging
import time
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch, helpers
from conf import date_format
from rq import Queue
from redis import Redis
from clean_url import job

logging.basicConfig(format='%(asctime)s - %(levelname)s : %(message)s', level=logging.INFO)
s = requests.Session()
s.trust_env = False
example = ["https://nyti.ms/4K9g6u",
           "https://youtu.be/5G9GOWlpP_0",
           "https://bbc.in/2Uuf8e4",
           "https://buff.ly/2Kmyv3z",
           "http://smarturl.it/1NOnlyAceBodyGood",
           "http://enth.to/2C8E5mf",
           "https://goo.gl/RSXZVd",
           "https://t.co/0mKpBINORi"]


if __name__ == "__main__":
    # for i, ex in enumerate(example):
    #     r = s.post("http://0.0.0.0:5000/", data=
    #         {
    #             "id": i,
    #             "url": ex
    #         }
    #         )
    #     logging.info(r)
    try:
        cache = Redis(host='redis', port=6379, decode_responses=True)
    except Exception:
        time.sleep(30)
        cache = Redis(host='redis', port=6379, decode_responses=True)
    queue_in = Queue('inputs', connection=cache, default_timeout=12)
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
                    req = {"id": doc["_id"], "created_at": doc["_source"]["created_at"], "url": url["expanded_url"]}
                    queue_in.enqueue(job, req)
                    # r = s.post("http://server:5000/", data={"id": doc["_id"], "url":url["expanded_url"]})
        start = start + timedelta(hours=1)
        if start > end:
            break
    logging.info("DONE")