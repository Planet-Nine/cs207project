import unittest

import multiprocessing
import time
from timeseries import TimeSeries
from tsdb.persistentdb import PersistentDB
from tsdb.tsdb_client import *
from tsdb.tsdb_server import TSDBServer
from tsdb.tsdb_error import *
import numpy as np
from scipy.stats import norm

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

class MyTest(unittest.TestCase):
    
    def test_badinput(self):
        with self.assertRaises(ValueError):
            db = PersistentDB({'pk':{'type':int}}, 'pk', dbname='testdb', overwrite=True)
        with self.assertRaises(ValueError):
            db = PersistentDB(schema, 'pk', dbname='testdb', overwrite=True, threshold='a')
        with self.assertRaises(ValueError):
            db = PersistentDB(schema, 12, dbname='testdb', overwrite=True, threshold='a')
        with self.assertRaises(ValueError):
            db = PersistentDB(schema, 'pk', dbname='testdb', overwrite=True, wordlength='a')
        with self.assertRaises(ValueError):
            db = PersistentDB(schema, 'pk', dbname='testdb', overwrite=True, threshold=-10)
        with self.assertRaises(ValueError):
            db = PersistentDB(schema, 'pk', dbname='testdb', overwrite=True, wordlength=-10)
        with self.assertRaises(ValueError):
            db = PersistentDB(schema, 'pk', dbname='testdb', overwrite=True, cardinality=-10)
        with self.assertRaises(ValueError):
            db = PersistentDB(schema, 'pk', dbname='testdb', overwrite=True, wordlength=10)
        with self.assertRaises(ValueError):
            db = PersistentDB(schema, 'pk', dbname='testdb', overwrite=True, tslen=300)
        with self.assertRaises(ValueError):
            db = PersistentDB(schema, 'pk', dbname='testdb', overwrite=True, tslen='256')
        with self.assertRaises(ValueError):
            db = PersistentDB(schema, 'pk', dbname='testdb', overwrite=True, tslen=8)
        with self.assertRaises(ValueError):
            db = PersistentDB(schema, 'pk', dbname='testdb', overwrite=True, cardinality=10.5)
        with self.assertRaises(ValueError):
            db = PersistentDB(schema, 'pk', dbname='testdb', overwrite=True, cardinality=10)
        with self.assertRaises(ValueError):
            db = PersistentDB(schema, 'pk', dbname='testdb', overwrite=True, cardinality=128)
        with self.assertRaises(ValueError):
            db = PersistentDB(schema, 'pk', dbname='testdb', load='yes')
        with self.assertRaises(ValueError):
            db = PersistentDB(schema, 'pk', dbname='testdb', overwrite='yes')
        with self.assertRaises(ValueError):
            db = PersistentDB(schema, 'pk', dbname=123, overwrite=True)

        with self.assertRaises(ValueError):
            db = PersistentDB({'pk':{'type':str, 'index':None}, 'DELETE':{'type':bool, 'index':1}}, 'pk', dbname='testdb', overwrite=True)
        with self.assertRaises(ValueError):
            db = PersistentDB({'pk':{'type':str, 'index':None}, 'mean:ie':{'type':float, 'index':1}}, 'pk', dbname='testdb', overwrite=True)
        with self.assertRaises(ValueError):
            db = PersistentDB({'pk':{'type':str, 'index':None}, 'mean':{'type':dict, 'index':1}}, 'pk', dbname='testdb', overwrite=True)
        with self.assertRaises(ValueError):
            db = PersistentDB([{'type':str, 'index':None}, {'type':float, 'index':1}], 'pk', dbname='testdb', overwrite=True)
        with self.assertRaises(ValueError):
            db = PersistentDB({'pk':{'type':int, 'index':None}, 'mean':{'type':float, 'index':1}}, 'pk', dbname='testdb', overwrite=True)
        with self.assertRaises(ValueError):
            db = PersistentDB({'pk':{'type':str, 'index':None}, 'd_vp-mean':{'type':float, 'index':1}}, 'pk', dbname='testdb', overwrite=True)
        with self.assertRaises(ValueError):
            db = PersistentDB({'pk':{'type':str, 'index':None}, 'vp':{'type':float, 'index':1}}, 'pk', dbname='testdb', overwrite=True)
        with self.assertRaises(ValueError):
            db = PersistentDB({'pk':{'type':str, 'index':None}, 'vp':{'type':bool, 'index':1}}, 'mean', dbname='testdb', overwrite=True)


    def test_db_tsinsert(self):
        ts1 = TimeSeries([1,2,3],[4,5,6])
        ts2 = TimeSeries([1,2,3],[4,5,6])
        db = PersistentDB(schema, 'pk', dbname='testdb', overwrite=True)
        db.insert_ts('ts1', ts1)
        with self.assertRaises(ValueError):
            db.insert_ts('ts1', ts2)
        with self.assertRaises(ValueError):
            db.insert_ts('ts:1', ts2)
        with self.assertRaises(ValueError):
            db.insert_ts('ts1', [[1,2,3],[4,5,6]])
        db.insert_ts('ts2', ts2)
        db.insert_ts('ts3', ts2)

    def test_db_upsertmeta(self):
        ts1 = TimeSeries([1,2,3],[4,5,6])
        db = PersistentDB(schema, 'pk', dbname='testdb', overwrite=True)
        with self.assertRaises(ValueError):
            db.upsert_meta('ts1', {'mean':5})
        db.insert_ts('ts1', ts1)
        with self.assertRaises(ValueError):
            db.upsert_meta('ts1', 'mean' == 5)
        db.upsert_meta('ts1', {'mean':5})

    def test_db_select(self):
        db = PersistentDB(schema, 'pk', dbname='testdb', overwrite=True)
        db.insert_ts('one', TimeSeries([1,2,3],[4,5,6]))
        db.insert_ts('two', TimeSeries([7,8,9],[3,4,5]))
        db.insert_ts('negone', TimeSeries([1,2,3],[-4,-5,-6]))
        db.upsert_meta('one', {'order':3})
        db.upsert_meta('one', {'order':1, 'mean':5})
        db.upsert_meta('two', {'order':2, 'mean':4})
        db.upsert_meta('negone', {'order':-1, 'mean':-5})

        with self.assertRaises(ValueError):
            db.select(meta=None, fields=None)
        with self.assertRaises(ValueError):
            db.select(meta=None, fields='mean')

        pks, fields = db.select(meta={}, fields=None)
        self.assertEqual(set(pks), set(['one', 'two', 'negone']))
        self.assertEqual(len(fields[0]), 0)

        pks, fields = db.select(meta={}, fields=[])
        self.assertEqual(set(pks), set(['one', 'two', 'negone']))
        self.assertEqual(fields[pks.index('one')]['order'], 1)

        pks, fields = db.select(meta={'mean':5}, fields=None)
        self.assertEqual(set(pks), set(['one']))
        pks, fields = db.select(meta={'mean':{'<=':4}}, fields=None)
        self.assertEqual(set(pks), set(['two', 'negone']))

        pks, fields = db.select(meta={'mean':5}, fields=['order'])
        self.assertEqual(fields[0]['order'], 1)

        pks, fields = db.select(meta={}, fields=None, additional={'sort_by':'+order'})
        self.assertEqual(pks, ['negone', 'one', 'two'])
        pks, fields = db.select(meta={}, fields=None, additional={'sort_by':'-order'})
        self.assertEqual(pks, ['two', 'one', 'negone'])
        pks, fields = db.select(meta={}, fields=None, additional={'sort_by':'-order', 'limit':2})
        self.assertEqual(pks, ['two', 'one'])
        
    def test_simsearch(self):
        db = PersistentDB(schema, 'pk', dbname='testdb', overwrite=True)
        n_add = 50
        mus = np.random.uniform(low=0.0, high=1.0, size=n_add)
        sigs = np.random.uniform(low=0.05, high=0.4, size=n_add)
        jits = np.random.uniform(low=0.05, high=0.2, size=n_add)
        for i, m, s, j in zip(range(n_add), mus, sigs, jits):
            db.insert_ts("ts-{}".format(i), tsmaker(m, s, j))

        m = np.random.uniform(low=0.0, high=1.0)
        s = np.random.uniform(low=0.05, high=0.4)
        j = np.random.uniform(low=0.05, high=0.2)
        query = tsmaker(m, s, j)

        with self.assertRaises(ValueError):  # No similarity search w/o vantage points
            closest = db.simsearch(query)

        for i in range(5):
            db.add_vp()

        closest = db.simsearch(query)

    def test_simsearchSAX(self):
        db = PersistentDB(schema, 'pk', dbname='testdb', overwrite=True)
        n_add = 50
        mus = np.random.uniform(low=0.0, high=1.0, size=n_add)
        sigs = np.random.uniform(low=0.05, high=0.4, size=n_add)
        jits = np.random.uniform(low=0.05, high=0.2, size=n_add)
        for i, m, s, j in zip(range(n_add), mus, sigs, jits):
            db.insert_ts("ts-{}".format(i), tsmaker(m, s, j))

        m = np.random.uniform(low=0.0, high=1.0)
        s = np.random.uniform(low=0.05, high=0.4)
        j = np.random.uniform(low=0.05, high=0.2)
        query = tsmaker(m, s, j)

        closest = db.simsearch_SAX(query)

    def test_trees(self):
        db = PersistentDB(schema, 'pk', dbname='testdb', overwrite=True)
        n_add = 50
        mus = np.random.uniform(low=0.0, high=1.0, size=n_add)
        sigs = np.random.uniform(low=0.05, high=0.4, size=n_add)
        jits = np.random.uniform(low=0.05, high=0.2, size=n_add)
        for i, m, s, j in zip(range(n_add), mus, sigs, jits):
            new_ts = tsmaker(m, s, j)
            db.insert_ts("ts-{}".format(i), tsmaker(m, s, j))
            db.upsert_meta("ts-{}".format(i), {'mean':new_ts.mean(), 'std':new_ts.std()})

        randi = set(np.random.randint(0,n_add, size=5))
        for i in randi:
            db.delete_ts("ts-{}".format(i))

        pks, fields = db.select(meta={'mean':{'<=':0.5}, 'std':{'>':2}}, fields=['mean', 'std'])
        for row in fields:
            self.assertLessEqual(row['mean'], 0.5)
            self.assertGreater(row['std'], 2)

    def test_load_del(self):
        db = PersistentDB(schema, 'pk', dbname='testdb', overwrite=True)
        n_add = 50
        mus = np.random.uniform(low=0.0, high=1.0, size=n_add)
        sigs = np.random.uniform(low=0.05, high=0.4, size=n_add)
        jits = np.random.uniform(low=0.05, high=0.2, size=n_add)
        saveinfo = {}
        for i, m, s, j in zip(range(n_add), mus, sigs, jits):
            new_ts = tsmaker(m, s, j)
            db.insert_ts("ts-{}".format(i), tsmaker(m, s, j))
            db.upsert_meta("ts-{}".format(i), {'mean':new_ts.mean(), 'std':new_ts.std()})
            saveinfo["ts-{}".format(i)] = new_ts.mean()

        db.add_vp("ts-4")
        db.add_vp()
        db.delete_ts("ts-4")
        pks, fields = db.select(meta={'vp':True}, fields=None)
        self.assertEqual(len(pks),1)

        newdb = PersistentDB(schema, 'pk', dbname='testdb', load=True)
        pks, fields = db.select(meta={}, fields=['mean'])
        self.assertEqual(len(pks), n_add-1)
        self.assertTrue("ts-4" not in pks)
        for i in range(0,n_add-1):
            self.assertEqual(fields[i]['mean'], saveinfo[pks[i]])

    ############## TEST WORKS ON LOCAL MACHINE BUT NOT IN TRAVIS #################################
    #def test_client_ops(self):
    #    schema["d_t3"] = {'convert': float, 'index': 1}
    #    db = DictDB(schema, 'pk')
    #    server = TSDBServer(db)
    #    def tests(self,t):
    #        client = TSDBClient()
    #        t1 = TimeSeries([0,1,2],[4,5,6])
    #        t2 = TimeSeries([0,1,2],[5,5,5.5])
    #        t3 = TimeSeries([0,1,2],[6,7,8])
    #        client.add_trigger('stats', 'insert_ts', ['mean', 'std'], None)
    #        client.insert_ts('t1',t1)
    #        client.remove_trigger('stats', 'insert_ts')
    #        client.add_trigger('corr', 'upsert_meta', ['d-t3'], t3)
    #        client.upsert_meta('t1',{'order':2, 'blarg':1})
    #        client.insert_ts('t2', t2)
    #        client.upsert_meta('t2',{'order':1, 'blarg':0})
    #        _, res = client.select(fields = ['mean'])
    #        self.assertTrue('t1' in res)
    #        self.assertTrue('mean' not in res['t2'])
    #        client.remove_trigger('corr', 'upsert_meta')
    #        client.insert_ts('t3', t3)
    #        client.upsert_meta('t3',{'order':1, 'blarg':0})
    #        _, res = client.select(fields = ['d-t3'])
    #        self.assertTrue('d-t3' not in res['t3'])
    #        _, res = client.select(fields=['mean','std'])
    #        self.assertEqual(5,res['t1']['mean'])
    #        self.assertEqual(t1.std(),res['t1']['std'])
    #        with self.assertRaises(TypeError):
    #            client.insert_ts(t1)
    #        _, res = client.insert_ts('t1',t1)
    #        self.assertEqual(_,TSDBStatus.INVALID_KEY)
    #        _, res = client.augmented_select('corr',['distance'],arg=t3, metadata_dict={'order':{'<':3}, 'blarg':{'<=':1}})
    #        self.assertTrue(res['t1']['distance'] < 1e-10)
    #        self.assertTrue(res['t2']['distance'] > 1e-10)
    #        with self.assertRaises(ValueError):
    #            _, res = client.augmented_select('corr',['distance'], metadata_dict={'order':{'<':3}, 'blarg':{'<=':1}})
    #        t.terminate()
        
    #    t = multiprocessing.Process(target=server.run)
    #    t.start()
    #    time.sleep(0.5)
    #    tests(self,t)
    #    t.terminate()
        
suite = unittest.TestLoader().loadTestsFromModule(MyTest())
unittest.TextTestRunner().run(suite)
