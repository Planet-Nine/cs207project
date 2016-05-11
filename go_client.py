#!/usr/bin/env python3
from tsdb import TSDBClient
import timeseries as ts
import numpy as np

from scipy.stats import norm

# m is the mean, s is the standard deviation, and j is the jitter
# the meta just fills in values for order and blarg from the schema
# def tsmaker(m, s, j):
#     "returns metadata and a time series in the shape of a jittered normal"
#     meta={}
#     meta['order'] = int(np.random.choice([-5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5]))
#     meta['blarg'] = int(np.random.choice([1, 2]))
#     t = np.arange(0.0, 1.0, 0.01)
#     v = norm.pdf(t, m, s) + j*np.random.randn(100)
#     return meta, ts.TimeSeries(t, v)

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

    # _, results = client.select(fields=['vp', 'd_vp-ts-25'])
    # # print(results)
    # for k in results:
    #      print(k, results[k])

    m = np.random.uniform(low=0.0, high=1.0)
    s = np.random.uniform(low=0.05, high=0.4)
    j = np.random.uniform(low=0.05, high=0.2)
    query = tsmaker(m, s, j)
    _, closest = client.simsearch(query)
    print("CLOSEST MATCH: ", closest)

    # import matplotlib.pyplot as plt
    # plt.plot(query.times(), query.values())
    # plt.plot(tsdict[nearestwanted].times(), tsdict[nearestwanted].values())
    # plt.show()

    # # dictionaries for time series and their metadata
    # tsdict={}
    # metadict={}
    # for i, m, s, j in zip(range(50), mus, sigs, jits):
    #     meta, tsrs = tsmaker(m, s, j)
    #     # the primary key format is ts-1, ts-2, etc
    #     pk = "ts-{}".format(i)
    #     tsdict[pk] = tsrs
    #     meta['vp'] = False # augment metadata with a boolean asking if this is a  VP.
    #     metadict[pk] = meta

    # # choose 5 distinct vantage point time series
    # vpkeys = ["ts-{}".format(i) for i in np.random.choice(range(50), size=5, replace=False)]
    # print("VPKEYS", vpkeys)
    # for i in range(5):
    #     # add 5 triggers to upsert distances to these vantage points
    #     client.add_trigger('corr', 'insert_ts', ["d_vp-{}".format(i)], tsdict[vpkeys[i]])
    #     # change the metadata for the vantage points to have meta['vp']=True
    #     metadict[vpkeys[i]]['vp']=True
    # # Having set up the triggers, now inser the time series, and upsert the metadata
    # for k in tsdict:
    #     client.insert_ts(k, tsdict[k])
    #     client.upsert_meta(k, metadict[k])

    # print("UPSERTS FINISHED")
    # print('---------------------')
    # print("STARTING SELECTS")

    # print('---------DEFAULT------------')
    # client.select()

    # #in this version, select has sprouted an additional keyword argument
    # # to allow for sorting. Limits could also be enforced through this.
    # print('---------ADDITIONAL------------')
    # client.select(additional={'sort_by': '-order'})

    # print('----------ORDER FIELD-----------')
    # _, results = client.select(fields=['order'])
    # for k in results:
    #     print(k, results[k])

    # print('---------ALL FILEDS------------')
    # client.select(fields=[])

    # print('------------TS with order 1---------')
    # client.select({'order': 1}, fields=['ts'])

    # print('------------All fields, blarg 1 ---------')
    # client.select({'blarg': 1}, fields=[])

    # print('------------order 1 blarg 2 no fields---------')
    # _, bla = client.select({'order': 1, 'blarg': 2})
    # print(bla)

    # print('------------order >= 4  order, blarg and mean sent back, also sorted---------')
    # _, results = client.select({'order': {'>=': 4}}, fields=['order', 'blarg', 'mean'], additional={'sort_by': '-order'})
    # for k in results:
    #     print(k, results[k])

    # print('------------order 1 blarg >= 1 fields blarg and std---------')
    # _, results = client.select({'blarg': {'>=': 1}, 'order': 1}, fields=['blarg', 'std'])
    # for k in results:
    #     print(k, results[k])

    # print('------now computing vantage point stuff---------------------')
    # print("VPS", vpkeys)

    # #we first create a query time series.
    # _, query = tsmaker(0.5, 0.2, 0.1)

    # # your code here begins

    # # Step 1: in the vpdist key, get  distances from query to vantage points
    # # this is an augmented select
    # _, results = client.augmented_select('corr', ['distance'], arg=query, metadata_dict={'vp': True})
    
    # #1b: choose the lowest distance vantage point
    # # you can do this in local code
    # least = 2
    # for key in results:
    #     if results[key]['distance'] < least:
    #         least = results[key]['distance']
    #         key_vp = key
    # dist_vp = results[key_vp]['distance']
    # ind = vpkeys.index(key_vp)
    # #closest_vp 
    # # Step 2: find all time series within 2*d(query, nearest_vp_to_query)
    # #this is an augmented select to the same proc in correlation
    # _, results2 = client.augmented_select('corr', ['distance'], arg=query, metadata_dict={'d_vp-{}'.format(ind): {'<=': 2*dist_vp}})
            
    # #{"d_vp-{}".format(ind): {'<=': 2*dist_vp}}
    # #2b: find the smallest distance amongst this ( or k smallest)
    # least = 2
    # for key in results2:
    #     if results2[key]['distance'] < least:
    #         least = results2[key]['distance']
    #         nearestwanted = key
    # #you can do this in local code
    # #your code here ends
    # # plot the timeseries to see visually how we did.
    # import matplotlib.pyplot as plt
    # plt.plot(query.times(), query.values())
    # plt.plot(tsdict[nearestwanted].times(), tsdict[nearestwanted].values())
    # plt.show()

if __name__=='__main__':
    main()
