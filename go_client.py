#!/usr/bin/env python3
from tsdb import TSDBClient
import timeseries as ts
import numpy as np
import procs.corr

from scipy.stats import norm

def tsmaker(m, s, j):
    "returns a time series in the shape of a jittered normal"
    t = np.arange(0.0, 1.0, 0.01)
    v = norm.pdf(t, m, s) + j*np.random.randn(100)
    return ts.TimeSeries(t, v)

def main():
    print('&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&')
    client = TSDBClient()

    n_add = 50
    mus = np.random.uniform(low=0.0, high=1.0, size=n_add)
    sigs = np.random.uniform(low=0.05, high=0.4, size=n_add)
    jits = np.random.uniform(low=0.05, high=0.2, size=n_add)
    for i, m, s, j in zip(range(n_add), mus, sigs, jits):
        client.insert_ts("ts-{}".format(i), tsmaker(m, s, j))

    for i in range(5):
         client.add_vp()

    _, results = client.select(fields=[])
    print("RESULT TYPE: ", type(results))
    for k in results:
         print(k, results[k])

    m = np.random.uniform(low=0.0, high=1.0)
    s = np.random.uniform(low=0.05, high=0.4)
    j = np.random.uniform(low=0.05, high=0.2)
    query = tsmaker(m, s, j)
    _, closest = client.simsearch(query)
    print("CLOSEST MATCH: ", closest)

    _, closest = client.sim_search_SAX(query)
    print("CLOSEST MATCH: ", closest)

if __name__=='__main__':
    main()
