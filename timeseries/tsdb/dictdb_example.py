from dictdb import DictDB
import numpy as np

schema = {'pk':{'index':None}, 'ts':{'index':None}, 'mean':{'index':3}, 'len':{'index':4}}
ddb = DictDB(schema)
fakets1 = np.arange(5)
fakets2 = np.arange(6)
fakets3 = np.arange(5)+1

ddb.insert_ts('fakets1', fakets1)
ddb.insert_ts('fakets2', fakets2)
ddb.insert_ts('fakets3', fakets3)

ddb.upsert_meta('fakets1', {'mean':np.mean(fakets1), 'len':len(fakets1)})
ddb.upsert_meta('fakets2', {'mean':np.mean(fakets2), 'len':len(fakets2)})
ddb.upsert_meta('fakets3', {'mean':np.mean(fakets3), 'len':len(fakets3)})

print(ddb.select({'len':5}))  # Should be {'fakets1', 'fakets3'}
print(ddb.select({'len':5, 'mean':2}))  # Should be {'fakets1'}
print(ddb.select({'len':5, 'mean':5}))  # Should be empty set
