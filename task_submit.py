import time

import tasks

def _parseMetric(metric):
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
        return None, None, None

def _processMetric(metric, datapoint):
    metric, tags = _parseMetric(metric)
    if metric is not None:
        tasks.queueMetric.delay(metric, datapoint, tags)

if __name__ == '__main__':
    while True:
        for i in xrange(1000):
            _processMetric("zuora.webapp.bbb.prod.1_aspose_prod_slv_zuora.storage.2271.save.hippo", (time.time(), 500,))
        time.sleep(1)
