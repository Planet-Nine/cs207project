#!/usr/bin/env python3
from tsdb import TSDBServer, PersistentDB
import timeseries as ts

schema = {
  'pk': {'type': str, 'index': None},  #will be indexed anyways
  'ts': {'index': None},
  'order': {'type': int, 'index': 1},
  'blarg': {'type': int, 'index': 1},
  'mean': {'type': float, 'index': 1},
  'std': {'type': float, 'index': 1},
  'vp': {'type': bool, 'index': 1}
}

def main():
    db = PersistentDB(schema, 'pk', load=False, overwrite=True)
    server = TSDBServer(db)
    server.run()

if __name__=='__main__':
    main()
