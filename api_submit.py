import time
import copy
import os
import requests
import random
from datadog import initialize, api


DD_API_KEY = os.getenv('DD_API_KEY', '<YOUR_API_KEY>')
DD_APP_KEY = os.getenv('DD_APP_KEY', '<YOUR_APP_KEY>')

options = {
    'api_key': DD_API_KEY,
    'app_key': DD_APP_KEY
}

METRIC_STORE = {}

initialize(**options)

def _sendMetrics(metric, val, tags):
    api.Metric.send(metric=metric, points=val, tags=tags)

def _parseMetrics():
    temp_store = copy.deepcopy(METRIC_STORE)
    global METRIC_STORE
    METRIC_STORE = {}
    all_metrics = []
    for metric, val in temp_store.iteritems():
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
            print e
    api.Metric.send(all_metrics)

def _processMetric(metric, datapoint):
    val = datapoint[1]
    if metric in METRIC_STORE:
        current = METRIC_STORE[metric]
        new_val = current + val
        METRIC_STORE[metric] = new_val
    else:
        METRIC_STORE[metric] = val

def _genMetrics():
    # generate some random metrics names
    d_list = ['abc', 'def', 'ghi', 'jkl', 'mno', 'pqr', 'stu', 'vwx']
    t_list = ['123', '456', '789', '101', '112', '131', '415']
    i_list = ['dsaf_asdf_asdf', 'qewr_qwer_qwer', 'cvxb_xcvb_xcbxv', 'hgjk_fghj_dfghj', 'asdfdgfh_sdfg_sdfg']
    d_len = len(d_list)
    t_len = len(t_list)
    i_len = len(i_list)
    met_list = []
    for i in range(d_len * t_len * i_len):
        t = t_list[random.randint(0, t_len-1)]
        d = d_list[random.randint(0, d_len-1)]
        n = i_list[random.randint(0, i_len-1)]
        met = "zuora.webapp." + d + ".prod." + n + ".storage." + t + ".save.hippo"
        met_list.append(met)
    return met_list

if __name__ == '__main__':
    met_list = _genMetrics()
    m_len = len(met_list)
    while True:
        start = time.time()
        for i in xrange(10000):
            met = met_list[random.randint(0, m_len-1)]
            _processMetric(met, (time.time(), 1,))
        _parseMetrics()
        print "time elapsed = {}".format(str(time.time() - start))
        time.sleep(1)
