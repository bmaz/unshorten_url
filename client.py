import requests
import logging

logging.basicConfig(format='%(asctime)s - %(levelname)s : %(message)s', level=logging.INFO)
s = requests.Session()
s.trust_env = False


if __name__ == "__main__":
    r = s.post("http://0.0.0.0:5000/", data=
        {
            "id": 0,
            "url": "https://lemde.fr/2VyDuzW",
            "file": "/usr/src/app/data/tmp.json"
        }
        )
    logging.info(r)
