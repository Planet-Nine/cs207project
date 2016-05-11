from persistentdb import PersistentDB
import sys
sys.path.insert(0, '../')   # This is sketchy AF but I'm not sure how else to do it
from timeseries import TimeSeries
import numpy as np
from scipy.stats import norm

identity = lambda x: x

schema = {
  'pk': {'type': str, 'index': None},  #will be indexed anyways
  'ts': {'index': None},
  'order': {'type': int, 'index': 1},
  'mean': {'type': float, 'index': 1},
  'std': {'type': float, 'index': 1},
  'vp': {'type': bool, 'index': 1}
}

def tsmaker(m, s, j):
    "returns metadata and a time series in the shape of a jittered normal"
    t = np.arange(0.0, 1.0, 0.01)
    v = norm.pdf(t, m, s) + j*np.random.randn(100)
    return TimeSeries(t, v)

db = PersistentDB(schema, 'pk', overwrite=True)

n_add = 50
mus = np.random.uniform(low=0.0, high=1.0, size=n_add)
sigs = np.random.uniform(low=0.05, high=0.4, size=n_add)
jits = np.random.uniform(low=0.05, high=0.2, size=n_add)
for i, m, s, j in zip(range(n_add), mus, sigs, jits):
    db.insert_ts("ts-{}".format(i+52), tsmaker(m, s, j))

# for i in range(4):
#     db.add_vp()

# m = np.random.uniform(low=0.0, high=1.0)
# s = np.random.uniform(low=0.05, high=0.4)
# j = np.random.uniform(low=0.05, high=0.2)
# closest = db.simsearch(tsmaker(m, s, j))
# print(closest)

# db.delete_ts('ts-4')

# db.insert_ts('one', TimeSeries([1,2,3],[4,5,6]))
# db.insert_ts('two', TimeSeries([7,8,9],[3,4,5]))
# db.insert_ts('negone', TimeSeries([1,2,3],[-4,-5,-6]))
# db.upsert_meta('one', {'order':1})
# db.upsert_meta('two', {'order':2})
# db.upsert_meta('negone', {'order':-1})

#db = PersistentDB(schema, 'pk', load=True)
#db.delete_ts('five')
#pks, fields = db.select(meta={}, fields=[])
#pks, fields = db.select(meta={}, additional={'sort_by':'-order', 'limit':2}, fields=[])
#pks, fields = db.select(meta={'order':{'<=':1}}, fields=[])
#print(pks)
#print(fields)
