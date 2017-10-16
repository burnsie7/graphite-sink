import os
from celery import Celery
import logging
from datadog import statsd

REDIS_HOST = os.getenv('REDIS_HOST', '<YOUR_REDIS_HOST>')
app = Celery('tasks', backend='redis://' + REDIS_HOST, broker='redis://' + REDIS_HOST)

@app.task
def queueMetric(metric, datapoint, tags):
    try:
        ts = datapoint[0]
        value = datapoint[1]
        statsd.gauge(metric, value, tags=tags)
    except Exception, e:
        logging.exception("Unparsable metric: {0}".format(metric))
        return None, None, None
