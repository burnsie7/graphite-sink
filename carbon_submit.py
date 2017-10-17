import time
import carbonclient
import random

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
        met = d + ".prod." + n + ".storage." + t + ".save.hippo"
        met_list.append(met)
    return met_list

met_list = _genMetrics()
m_len = len(met_list)

while True:
    try:
        met = met_list[random.randint(0, m_len-1)]
        carbonclient.update(carbonserver = "127.0.0.1", carbonport = 2013, server="webapp", group = "zuora", metric = met, value = 1, epoch=time.time() )
    except carbonclient.ConnectionError:
        time.sleep(1)
