import requests
import logging

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
    for i, ex in enumerate(example):
        r = s.post("http://0.0.0.0:5000/", data=
            {
                "id": i,
                "url": ex
            }
            )
        logging.info(r)
