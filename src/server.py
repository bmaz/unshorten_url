from redis import Redis
from flask import Flask, Response, request
import json
from rq import Queue
from clean_url import job, return_urlparse
import logging

logging.basicConfig(format='%(asctime)s - %(levelname)s : %(message)s', level=logging.INFO)

app = Flask(__name__)
redis = Redis(host='redis', port=6379)
queue_in = Queue('inputs', connection=redis)


@app.route("/", methods=["POST"])
@app.route('/<path:short_url>', methods=["GET"])
def route(short_url=None):
    if request.method == "GET":
        return Response(response=json.dumps(return_urlparse(short_url)), status=200, mimetype="application/json")
    else:
        res = queue_in.enqueue(job, request.form)
        return str(res)


if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
