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
        
    
suite = unittest.TestLoader().loadTestsFromModule(MyTest())
unittest.TextTestRunner().run(suite)
