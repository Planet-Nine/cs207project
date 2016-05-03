import unittest

import multiprocessing
import time
from timeseries import TimeSeries
from tsdb.dictdb import DictDB
from tsdb.tsdb_client import *
from tsdb.tsdb_server import TSDBServer
from tsdb.tsdb_error import *


import numpy as np

identity = lambda x: x

schema = {
  'pk': {'convert': identity, 'index': None},  #will be indexed anyways
  'ts': {'convert': identity, 'index': None},
  'order': {'convert': int, 'index': 1},
  'blarg': {'convert': int, 'index': 1},
  'useless': {'convert': identity, 'index': None},
  'mean': {'convert': float, 'index': 1},
  'std': {'convert': float, 'index': 1},
  'vp': {'convert': bool, 'index': 1}
}

class MyTest(unittest.TestCase):
    
    def test_db_tsinsert(self):
        ts1 = TimeSeries([1,2,3],[4,5,6])
        ts2 = TimeSeries([1,2,3],[4,5,6])
        db = DictDB(schema, 'pk')
        db.insert_ts('ts1', ts1)
        with self.assertRaises(ValueError):
            db.insert_ts('ts1', ts2)
        db.insert_ts('ts2', ts2)
        db.insert_ts('ts3', ts2)

    def test_db_upsertmeta(self):
        ts1 = TimeSeries([1,2,3],[4,5,6])
        db = DictDB(schema, 'pk')
        with self.assertRaises(ValueError):
            db.upsert_meta('ts1', {'mean':5})
        db.insert_ts('ts1', ts1)
        with self.assertRaises(ValueError):
            db.upsert_meta('ts1', 'mean' == 5)
        db.upsert_meta('ts1', {'mean':5})

    def test_db_select(self):
        db = DictDB(schema, 'pk')
        db.insert_ts('one', TimeSeries([1,2,3],[4,5,6]))
        db.insert_ts('two', TimeSeries([7,8,9],[3,4,5]))
        db.insert_ts('negone', TimeSeries([1,2,3],[-4,-5,-6]))
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
        
    def test_client_ops(self):
        schema["d_t3"] = {'convert': float, 'index': 1}
        db = DictDB(schema, 'pk')
        server = TSDBServer(db)
        def tests(self,t):
            client = TSDBClient()
            t1 = TimeSeries([0,1,2],[4,5,6])
            t2 = TimeSeries([0,1,2],[5,5,5.5])
            t3 = TimeSeries([0,1,2],[6,7,8])
            client.add_trigger('stats', 'insert_ts', ['mean', 'std'], None)
            client.insert_ts('t1',t1)
            client.remove_trigger('stats', 'insert_ts')
            client.add_trigger('corr', 'upsert_meta', ['d-t3'], t3)
            client.upsert_meta('t1',{'order':2, 'blarg':1})
            client.insert_ts('t2', t2)
            client.upsert_meta('t2',{'order':1, 'blarg':0})
            _, res = client.select(fields = ['mean'])
            self.assertTrue('t1' in res)
            self.assertTrue('mean' not in res['t2'])
            client.remove_trigger('corr', 'upsert_meta')
            client.insert_ts('t3', t3)
            client.upsert_meta('t3',{'order':1, 'blarg':0})
            _, res = client.select(fields = ['d-t3'])
            self.assertTrue('d-t3' not in res['t3'])
            _, res = client.select(fields=['mean','std'])
            self.assertEqual(5,res['t1']['mean'])
            self.assertEqual(t1.std(),res['t1']['std'])
            with self.assertRaises(TypeError):
                client.insert_ts(t1)
            _, res = client.insert_ts('t1',t1)
            self.assertEqual(_,TSDBStatus.INVALID_KEY)
            _, res = client.augmented_select('corr',['distance'],arg=t3, metadata_dict={'order':{'<':3}, 'blarg':{'<=':1}})
            self.assertTrue(res['t1']['distance'] < 1e-10)
            self.assertTrue(res['t2']['distance'] > 1e-10)
            t.terminate()
        
        t = multiprocessing.Process(target=server.run)
        t.start()
        time.sleep(0.5)
        tests(self,t)
        
        
        
    #def test_upsert_meta(self):
    #    db = DictDB(schema, 'pk')
    #    server = TSDBServer(db)
    #    def tests(self,t):
    #        client = TSDBClient()
    #        t1 = TimeSeries([0,1,2],[4,5,6])
    #        client.insert_ts('t1',t1)
    #        client.upsert_meta('t1',{'order':2})
    #        with self.assertRaises(TypeError):
    #            client.upsert_meta(t1)
            #with self.assertRaises(Exception):
            #    client.upsert_meta('t2',t1)
            #with self.assertRaises(KeyError):
            #    client.upsert_meta('t1',{'data':2}) 
    #        t.terminate()
    #    t = multiprocessing.Process(target=server.run)
    #    t.start()
    #    time.sleep(0.5)
    #    tests(self,t)
        #print('process',self.t.terminate())
    #    t.terminate()
        #with 
        #s.start()
        #q.start()
        

    #def test_select(self):
    #    db = DictDB(schema, 'pk')
    #    server = TSDBServer(db)
    #    def tests(self,t):
    #        client = TSDBClient()
    #        t1 = TimeSeries([0,1,2],[4,5,6])
    #        client.insert_ts('t1',t1)
    #        client.upsert_meta('t1',{'order':2})
    #        _, res = client.select(fields=['ts'])
    #        self.assertEqual(t1,TimeSeries(res['t1']['ts']))
    #        _, res = client.select(fields=['order'])
    #        self.assertEqual(2,res['t1']['order'])
    #        with self.assertRaises(ValueError):
    #            _, res = client.select(fields='garden')
    #        t.terminate()
    #    t = multiprocessing.Process(target=server.run)
    #    t.start()
    #    tests(self,t)
    #    t.terminate()
        
    
    
    #def test_augmented_select(self):
    #    db = DictDB(schema, 'pk')
    #    server = TSDBServer(db)
    #    def tests(self,t):
    #        client = TSDBClient()
    #        t1 = TimeSeries([0,1,2],[4,5,6])
    #        t2 = TimeSeries([0,1,2],[5,5,5.5])
    #        t3 = TimeSeries([0,1,2],[6,7,8])
    #        client.insert_ts('t1',t1)
    #        client.upsert_meta('t1',{'order':2, 'blarg':1})
    #        client.insert_ts('t2',t2)
    #        client.upsert_meta('t2',{'order':1, 'blarg':1})
    #        _, res = client.augmented_select('corr',['distance'],arg=t3, metadata_dict={'order':{'<':3}, 'blarg':1})
    #        self.assertTrue(res['t1']['distance'] < 1e-10)
    #        self.assertTrue(res['t2']['distance'] > 1e-10)
    #        t.terminate()
    #    t = multiprocessing.Process(target=server.run)
    #    t.start()
    #    tests(self,t)
    #    t.terminate()
        

    #def test_add_trigger(self):
    #    db = DictDB(schema, 'pk')
    #    server = TSDBServer(db)
    #    def tests(self,t):
    #        client = TSDBClient()
    #        t1 = TimeSeries([0,1,2],[4,5,6])
    #        t2 = TimeSeries([0,1,2],[5,5,5.5])
    #        t3 = TimeSeries([0,1,2],[6,7,8])
    #        client.add_trigger('stats', 'insert_ts', ['mean', 'std'], None)
    #        client.insert_ts('t1',t1)
    #        _, res = client.select(fields=['mean','std'])
    #        self.assertEqual(5,res['t1']['mean'])
    #        self.assertEqual(t1.std(),res['t1']['std'])
    #        client.add_trigger('corr', 'upsert_meta', ['d-t3'], t3)
    #        client.upsert_meta('t1',{'order':2, 'blarg':1})
    #        client.insert_ts('t2', t2)
    #        client.upsert_meta('t2',{'order':1, 'blarg':0})
    #        _, res = client.select(metadata_dict={'order':{'<=':2},'blarg':1}, fields = ['d-t3'])
    #        self.assertTrue(res['t1']['d-t3'] < 1e-9)
    #        self.assertTrue('t2' not in res)
    #        with self.assertRaises(ValueError):
    #            _, res = client.select(fields=['d-t1'])
    #        t.terminate()
    #    t = multiprocessing.Process(target=server.run)
    #    t.start()
    #    tests(self,t)
    #    t.terminate()
    
    #def test_remove_trigger(self):
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
    #        self.assertTrue('t2' not in res)
    #        client.remove_trigger('corr', 'upsert_meta')
    #        client.insert_ts('t3', t3)
    #        client.upsert_meta('t3',{'order':1, 'blarg':0})
    #        _, res = client.select(fields = ['d-t3'])
    #        with self.assertRaises(KeyError):
    #            res['t3']
    #        t.terminate()
    #    t = multiprocessing.Process(target=server.run)
    #    t.start()
    #    tests(self,t)
    #    t.terminate()
     
suite = unittest.TestLoader().loadTestsFromModule(MyTest())
unittest.TextTestRunner().run(suite)
