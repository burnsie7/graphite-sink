import time
import tasks
import copy
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
    api.Metric.send(all_metrics)

def _processMetric(metric, datapoint):
    val = datapoint[1]
    if metric in METRIC_STORE:
        current = METRIC_STORE[metric]
        new_val = current + val
        METRIC_STORE[metric] = new_val
    else:
        METRIC_STORE[metric] = val

if __name__ == '__main__':
    dcs = ['abc', 'def', 'ghi', 'jkl', 'mno']
    dlen = len(dcs)
    tid = ['123', '456', '789', '111', '234', '456']
    tlen = len(tid)
    ics = ['dsaf_asdf_asdf', 'qewr_qwer_qwer', 'cvxb_xcvb_xcbxv', 'hgjk_fghj_dfghj']
    ilen = len(ics)
    met_list = []
    for i in range(200):
        t = tid[random.randint(0, tlen-1)]
        d = dcs[random.randint(0, dlen-1)]
        n = ics[random.randint(0, ilen-1)]
        met = "zuora.webapp." + d + ".prod." + n + ".storage." + t + ".save.hippo"
        met_list.append(met)
    mlen = len(met_list)
    start = time.time()
    for i in xrange(100000):
        met = met_list[random.randint(0, mlen-1)]
        _processMetric(met, (time.time(), 1,))
    print "proc = {}".format(str(time.time()-start))
    print "len = {}".format(str(len(METRIC_STORE)))
    _parseMetrics()
    end = time.time()
    print "total = {}".format(str(end-start))
    #time.sleep(1)
    #while True:
    #    for i in xrange(1000):
    #        _processMetric("zuora.webapp.bbb.prod.1_aspose_prod_slv_zuora.storage.2271.save.hippo", (time.time(), 500,))
    #    _parseMetrics()
    #    time.sleep(1)
