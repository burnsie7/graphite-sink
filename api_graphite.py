# (C) Datadog, Inc. 2010-2016
# All rights reserved
# Licensed under Simplified BSD License (see LICENSE)

# stdlib
from argparse import ArgumentParser
import cPickle as pickle
import copy
import logging
import os
import threading
import time
import struct
import sys

from tornado.ioloop import IOLoop
from tornado.tcpserver import TCPServer
from tornado import netutil, process

from datadog import api, initialize

log = logging.getLogger(__name__)
out_hdlr = logging.StreamHandler(sys.stdout)
out_hdlr.setFormatter(logging.Formatter('%(asctime)s %(message)s'))
out_hdlr.setLevel(logging.INFO)
log.addHandler(out_hdlr)
log.setLevel(logging.INFO)

METRIC_STORE = {}

DD_API_KEY = os.getenv('DD_API_KEY', '<YOUR_API_KEY>')
DD_APP_KEY = os.getenv('DD_APP_KEY', '<YOUR_APP_KEY>')

options = {
    'api_key': DD_API_KEY,
    'app_key': DD_APP_KEY
}

initialize(**options)

def get_and_clear_store():
    global METRIC_STORE
    temp_store = copy.deepcopy(METRIC_STORE)
    METRIC_STORE = {}
    return temp_store

class GraphiteServer(TCPServer):

    def __init__(self, io_loop=None, ssl_options=None, **kwargs):
        TCPServer.__init__(self, io_loop=io_loop, ssl_options=ssl_options, **kwargs)
        self._sendMetrics()

    def _sendMetrics(self):
        temp_store = get_and_clear_store()
        all_metrics = []
        start_time = time.time()
        for metric, val in temp_store.iteritems():
            if metric.startswith('zuora.webapp'):
                try:
                    tags = []
                    components = metric.split('.')

                    datacenter = 'datacenter:' + components.pop(2)
                    env = 'env:' + components.pop(2)
                    instance = 'instance:' + components.pop(2)
                    sub_instance = 'subinstance:' + instance.split('_')[1]
                    tenant_id = 'tenant_id:' + components.pop(3)
                    tags = [datacenter, env, instance, sub_instance, tenant_id]

                    metric = '.'.join(components)
                    all_metrics.append({'metric': metric, 'points': val, 'tags': tags})
                except Exception as e:
                    log.error(e)
        if len(all_metrics):
            log.debug(str(temp_store))
            api.Metric.send(all_metrics)
            log.info("sent {} metrics in {} seconds\n".format(str(len(all_metrics)), str(time.time() - start_time)))
        else:
            log.info("no metrics received")
        threading.Timer(10, self._sendMetrics).start()

    def handle_stream(self, stream, address):
        GraphiteConnection(stream, address)


class GraphiteConnection(object):

    def __init__(self, stream, address):
        log.info('received a new connection from {}'.format(address))
        self.stream = stream
        self.address = address
        self.stream.set_close_callback(self._on_close)
        self.stream.read_bytes(4, self._on_read_header)

    def _on_read_header(self, data):
        try:
            size = struct.unpack("!L", data)[0]
            log.debug("Receiving a string of size: {}".format(str(size)))
            self.stream.read_bytes(size, self._on_read_line)
        except Exception as e:
            log.error(e)

    def _on_read_line(self, data):
        log.debug('read a new line')
        self._decode(data)

    def _on_close(self):
        log.info('client quit')

    def _processMetric(self, metric, datapoint):
        if metric is not None:
            try:
                val = datapoint[1]
                if metric in METRIC_STORE:
                    current = METRIC_STORE[metric]
                    new_val = current + val
                    METRIC_STORE[metric] = new_val
                else:
                    METRIC_STORE[metric] = val
            except Exception as e:
                log.error(e)

    def _decode(self, data):

        try:
            datapoints = pickle.loads(data)
        except Exception:
            log.exception("Cannot decode grapite points")
            return

        for (metric, datapoint) in datapoints:
            try:
                datapoint = (float(datapoint[0]), float(datapoint[1]))
            except Exception as e:
                log.error(e)
                continue

            self._processMetric(metric, datapoint)

        self.stream.read_bytes(4, self._on_read_header)


def start_graphite_listener(port):

    echo_server = GraphiteServer()
    echo_server.listen(port)
    IOLoop.instance().start()

if __name__ == '__main__':

    parser = ArgumentParser(description='run a tornado graphite sink')
    parser.add_argument('port', help='port num')
    args = parser.parse_args()
    port = args.port
    start_graphite_listener(port)
