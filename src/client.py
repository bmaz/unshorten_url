import requests
import logging
import time
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch, helpers
from rabbit_queue import RabbitQueue
from conf import date_format

logging.basicConfig(format='%(asctime)s - %(levelname)s : %(message)s', level=logging.INFO)

if __name__ == "__main__":
    start = datetime(2018, 12, 1, 8, 0, 0)
    end = datetime(2018, 12, 31, 23, 59, 59)
    es = Elasticsearch("otmedia-srv01.priv.ina:9292")
    queue_in = RabbitQueue('inputs')

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

    while start < end:
        query = build_query(start)
        logging.info(query["query"]["bool"]["filter"])
        scan = helpers.scan(client=es, index="otmtweets-2018-12*", query=query)
        counter = 0
        for doc in scan:
            counter += 1
            for url in doc["_source"]["entities"]["urls"]:
                if not url["expanded_url"].startswith("https://twitter.com/"):
                    req = {"id": doc["_id"], "created_at": doc["_source"]["created_at"], "url": url["expanded_url"]}
                    queue_in.publish(req)
        start = start + timedelta(hours=1)
    logging.info("DONE")
