from collections import defaultdict
from operator import and_
from functools import reduce
import operator

OPMAP = {
    '<': operator.lt,
    '>': operator.le,
    '==': operator.eq,
    '!=': operator.ne,
    '<=': operator.le,
    '>=': operator.ge
}


def metafiltered(d, schema, fieldswanted):
    d2 = {}
    if len(fieldswanted) == 0:
        keys = [k for k in d.keys() if k != 'ts']
    else:
        keys = [k for k in d.keys() if k in fieldswanted]
    for k in keys:
        if k in schema:
            d2[k] = schema[k]['convert'](d[k])
    return d2


class DictDB:
    "Database implementation in a dict"
    def __init__(self, schema, pkfield):
        self.indexes = {}
        self.rows = {}
        self.schema = schema
        self.pkfield = pkfield
        for s in schema:
            indexinfo = schema[s]['index']
            # convert = schema[s]['convert']
            # later use binary search trees for highcard/numeric
            # bitmaps for lowcard/str_or_factor
            if indexinfo is not None:
                self.indexes[s] = defaultdict(set)

    def insert_ts(self, pk, ts):
        if pk not in self.rows:
            self.rows[pk] = {'pk': pk}
        else:
            raise ValueError('Duplicate primary key found during insert')
        self.rows[pk]['ts'] = ts
        self.update_indices(pk)

    def upsert_meta(self, pk, meta):
        if pk not in self.rows:
            self.rows[pk] = {'pk': pk}
        for field in meta:
            if field not in self.schema:
                raise ValueError('Field not supported by schema')
            else:
                self.rows[pk][field] = meta[field]
        self.update_indices(pk)

    def index_bulk(self, pks=[]):
        if len(pks) == 0:
            pks = self.rows
        for pkid in self.pks:
            self.update_indices(pkid)

    def update_indices(self, pk):
        row = self.rows[pk]
        for field in row:
            v = row[field]
            if self.schema[field]['index'] is not None:
                idx = self.indexes[field]
                idx[v].add(pk)

    def select(self, meta, fields, additional={}):
        # if fields is None: return only pks
        # like so [pk1,pk2],[{},{}]
        # if fields is [], this means all fields
        #except for the 'ts' field. Looks like
        #['pk1',...],[{'f1':v1, 'f2':v2},...]
        # if the names of fields are given in the list, include only those fields. `ts` ia an
        #acceptable field and can be used to just return time series.
        #see tsdb_server to see how this return
        #value is used
        #additional is a dictionary. It has two possible keys:
        #(a){'sort_by':'-order'} or {'sort_by':'+order'} where order
        #must be in the schema AND have an index. (b) limit: 'limit':10
        #which will give you the top 10 in the current sort order.
        #your code here


        # Find primary keys for timeseries which match metadata
        # If no metadata provided, return all rows
        if len(meta) == 0:
            pks = self.rows.keys()
        # Otherwise, search for matching rows
        else:
            first = True
            for field in meta:
                # Define operator  (For now assuming just one per field)
                if type(meta[field]) == dict:
                    for opkey in meta[field]:
                        op = OPMAP[opkey]
                        compval = meta[field][opkey]
                else:
                    op = OPMAP['==']
                    compval = meta[field]

                pks_field = []
                if field not in self.schema:
                    raise ValueError('Field not supported by schema')
                else:
                    for val in self.indexes[field]:
                        if op(val,compval):
                            pks_field = pks_field + list(self.indexes[field][val])
                    if first: 
                        pks = set(pks_field)
                        first = False
                    else:
                        pks = pks.intersection(set(pks_field))
            pks = list(pks)

        # Retrieve appropriate fields
        matchfields = []
        # Return only pks
        if fields is None:
            for pk in pks: matchfields.append({})
        # Return all metadata
        elif len(fields) == 0:   
            for pk in pks:
                pkfields = {}
                pkrow = self.rows[pk]
                allfields = self.rows[pk].keys()
                for f in allfields:
                    if f != 'ts':
                        pkfields[f] = pkrow[f]
                matchfields.append(pkfields)
        else:
            for pk in pks:
                pkfields = {}
                pkrow = self.rows[pk]
                for f in fields:
                    pkfields[f] = pkrow[f]
                matchfields.append(pkfields)

        return pks, matchfields
