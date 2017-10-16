# (C) Datadog, Inc. 2010-2016
# All rights reserved
# Licensed under Simplified BSD License (see LICENSE)

# stdlib
import cPickle as pickle
import struct
import sys

import tasks

from tornado.ioloop import IOLoop
from tornado.tcpserver import TCPServer
from tornado import netutil
from tornado import process

from argparse import ArgumentParser


class GraphiteServer(TCPServer):

    def __init__(self, io_loop=None, ssl_options=None, **kwargs):
        TCPServer.__init__(self, io_loop=io_loop, ssl_options=ssl_options, **kwargs)

    def handle_stream(self, stream, address):
        GraphiteConnection(stream, address)


class GraphiteConnection(object):

    def __init__(self, stream, address):
        sys.stdout.write('received a new connection from {}'.format(address))
        self.stream = stream
        self.address = address
        self.stream.set_close_callback(self._on_close)
        self.stream.read_bytes(4, self._on_read_header)

    def _on_read_header(self, data):
        try:
            size = struct.unpack("!L", data)[0]
            sys.stdout.write("Receiving a string of size: {}".format(str(size)))
            self.stream.read_bytes(size, self._on_read_line)
        except Exception as e:
            log.error(e)

    def _on_read_line(self, data):
        sys.stdout.write('read a new line')
        self._decode(data)

    def _on_close(self):
        sys.stdout.write('client quit')

    def _parseMetric(self, metric):

        try:
            components = metric.split('.')
            tags = []
            if metric.startswith("zuora.webapp"):
                datacenter = components.pop(2)
                env = components.pop(2)
                instance = components.pop(2)
                sub_instance = instance.split('_')[1]
                tenant_id = components.pop(3)
                metric = ".".join(components)
                tags = ['datacenter:{}'.format(datacenter), 'env:{}'.format(env),
                        'instance:{}'.format(instance), 'sub_instance:{}'.format(sub_instance), 'tenant_id:{}'.format(tenant_id)]

            metric = metric

            return metric, tags
        except Exception:
            log.exception("Unparsable metric: %s" % metric)
            return None, None, None

    def _processMetric(self, metric, datapoint):

        metric, tags = self._parseMetric(metric)
        sys.stdout.write("Processed metric {} {}".format(metric, datapoint))
        if metric is not None:
            tasks.queueMetric.delay(metric, datapoint, tags)
            sys.stdout.write("Posted metric: {}".format(metric))
        sys.stdout.flush()

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
