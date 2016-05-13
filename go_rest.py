#!/usr/bin/env python3
from tsdb import TSDBClient
import timeseries as ts
import numpy as np
import requests
from scipy.stats import norm
import urllib

def tsmaker(m, s, j):
    "returns a time series in the shape of a jittered normal"
    t = np.arange(0.0, 1.0, 0.01)
    v = norm.pdf(t, m, s) + j*np.random.randn(100)
    return ts.TimeSeries(t, v)
class RestClient:
    def insert_ts(self, primary_key, ts):
        return requests.post("http://localhost:8080/insert_ts",params=urllib.parse.urlencode({'pk':primary_key,'ts':ts.to_json()}))
    def add_vp(self, primary_key=None):
        if primary_key==None:
            return requests.post("http://localhost:8080/add_vp")
        else: 
            return requests.post("http://localhost:8080/add_vp",params=urllib.parse.urlencode({'pk':primary_key}))
    def select(self, meta_dict={}, fields=None, additional=None):
        return requests.get("http://localhost:8080/select",params=urllib.parse.urlencode({'meta_dict':meta_dict,'fields':fields,'additional':additional}))
    def augmented_select(self, proc, target, arg=None, meta_dict={}, additional=None):
        return requests.get("http://localhost:8080/augmented_select",params=urllib.parse.urlencode({'proc':proc,'target':target,'arg':arg.to_json(),'meta_dict':meta_dict,'additional':additional}))
        
    def simsearch(self, ts):
        return requests.get("http://localhost:8080/simsearch",params=urllib.parse.urlencode({'ts':ts.to_json()}))
    
def main():
    print('&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&')
    client=RestClient()
    n_add = 50
    mus = np.random.uniform(low=0.0, high=1.0, size=n_add)
    sigs = np.random.uniform(low=0.05, high=0.4, size=n_add)
    jits = np.random.uniform(low=0.05, high=0.2, size=n_add)
    for i, m, s, j in zip(range(n_add), mus, sigs, jits):
        client.insert_ts("ts-{}".format(i), tsmaker(m, s, j))

    for i in range(5):
        client.add_vp()

    _, results = client.select(fields=['vp', 'd_vp-ts-25'])
    for k in results:
         print(k, results[k])

    m = np.random.uniform(low=0.0, high=1.0)
    s = np.random.uniform(low=0.05, high=0.4)
    j = np.random.uniform(low=0.05, high=0.2)
    query = tsmaker(m, s, j)
    response = client.simsearch(query)
    print(response.status_code)
    print(response.text)

if __name__=='__main__':
    main()
