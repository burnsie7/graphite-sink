# (C) Datadog, Inc. 2010-2016
# All rights reserved
# Licensed under Simplified BSD License (see LICENSE)

# stdlib
import cPickle as pickle
import struct
import sys
import os

import threading
import tasks
import time
import copy

from tornado.ioloop import IOLoop
from tornado.tcpserver import TCPServer
from tornado import netutil
from tornado import process

from argparse import ArgumentParser
from datadog import api, initialize

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
        sys.stdout.write(" - sent\n")
        sys.stdout.write(str(temp_store))
        sys.stdout.flush()
        all_metrics = []
        for metric, val in temp_store.iteritems():
            try:
                start = time.time()
                components = metric.split('.')
                metric_parts = [components[0], components[1], components[5], components[7], components[8]]
                tags = []
                datacenter = 'datacenter:' + components[2]
                env = 'env:' + components[3]
                instance = 'instance:' + components[4]
                sub_instance = 'subinstance:' + instance.split('_')[1]
                tenant_id = 'tenant_id:' + components[6]
                metric = ".".join(metric_parts)
                tags = [datacenter, env, instance, sub_instance, tenant_id]
                all_metrics.append({'metric': metric, 'points': val, 'tags': tags})
                print "parse = {}".format(str(time.time()-start))
                print "send  = {}".format(str(time.time()-start))
            except Exception as e:
                print e
        if len(all_metrics):
            api.Metric.send(all_metrics)
        threading.Timer(10, self._sendMetrics).start()

    def handle_stream(self, stream, address):
        GraphiteConnection(stream, address)


class GraphiteConnection(object):

    def __init__(self, stream, address):
        #sys.stdout.write('received a new connection from {}'.format(address))
        self.stream = stream
        self.address = address
        self.stream.set_close_callback(self._on_close)
        self.stream.read_bytes(4, self._on_read_header)

    def _on_read_header(self, data):
        try:
            size = struct.unpack("!L", data)[0]
            #sys.stdout.write("Receiving a string of size: {}".format(str(size)))
            self.stream.read_bytes(size, self._on_read_line)
        except Exception as e:
            log.error(e)

    def _on_read_line(self, data):
        #sys.stdout.write('read a new line')
        self._decode(data)

    def _on_close(self):
        sys.stdout.write('client quit')
        sys.stdout.flush()

    def _processMetric(self, metric, datapoint):

        #sys.stdout.write("Processed metric {} {}".format(metric, datapoint))
        if metric is not None:
            val = datapoint[1]
            if metric in METRIC_STORE:
                current = METRIC_STORE[metric]
                new_val = current + val
                METRIC_STORE[metric] = new_val
            else:
                METRIC_STORE[metric] = val
            #tasks.queueMetric.delay(metric, datapoint, tags)
            #sys.stdout.write("Posted metric: {}".format(metric))

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
