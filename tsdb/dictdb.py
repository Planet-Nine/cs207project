from collections import defaultdict, OrderedDict
from operator import and_
from functools import reduce
import operator
import sys
sys.path.insert(0, '../')   # This is sketchy AF but I'm not sure how else to do it
from timeseries import TimeSeries

OPMAP = {
    '<': operator.lt,
    '>': operator.gt,
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
        if not isinstance(ts, TimeSeries):
            raise ValueError('Must insert a TimeSeries object')
        self.rows[pk]['ts'] = ts
        self.update_indices(pk)

    def upsert_meta(self, pk, meta):
        # Test that metadata has correct structure
        if isinstance(meta, dict) == False:
            raise ValueError('Metadata should be in the form of a dictionary')
        if pk not in self.rows:
            raise ValueError('Timeseries should be added prior to metadata')
        for field in meta:
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

    def select(self, meta, fields, additional=None):
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

        # Enforce appropriate input
        if isinstance(meta, dict) == False:
            raise ValueError('Metadata should be in the form of a dictionary')
        if fields is not None and isinstance(fields, list) == False:
            raise ValueError('Fields should either be in list form or None')


        sort = 0
        limit = None
        # Allows nonsense input into additional, just ignores it
        if additional is not None:
            if 'sort_by' in additional and 'order' in self.schema and self.schema['order']['index'] is not None:
                if additional['sort_by'] == '-order': sort = -1
                if additional['sort_by'] == '+order': sort = 1
            if len(additional) > 1 and 'limit' in additional:
                limit = int(additional['limit'])

        # Find primary keys for timeseries which match metadata
        # If no metadata provided, return all rows
        if len(meta) == 0:
            pks = list(self.rows.keys())
        # Otherwise, search for matching rows
        else:
            first = True
            for field in meta:
                # Define operator  (For now assuming just one per field)
                if type(meta[field]) == dict or type(meta[field]) == OrderedDict:
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
        orderfield = []
        # Return only pks
        if fields is None:
            for pk in pks: 
                matchfields.append({})
                if 'order' in self.rows[pk] and sort != 0:         
                    orderfield.append(self.rows[pk]['order']*sort)
                else:
                    orderfield.append(float('inf'))    # This ensures reasonable behavior
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
                if 'order' in self.rows[pk] and sort != 0:         
                    orderfield.append(self.rows[pk]['order']*sort)
                else:
                    orderfield.append(float('inf'))    # This ensures reasonable behavior if order is not defined for that pk
        # Return specific metadata
        else:
            for pk in pks:
                pkfields = {}
                pkrow = self.rows[pk]
                for f in fields:
                    if f in pkrow:
                        pkfields[f] = pkrow[f]
                matchfields.append(pkfields)
                if 'order' in self.rows[pk] and sort != 0:
                    orderfield.append(self.rows[pk]['order']*sort)
                else:
                    orderfield.append(float('inf'))

        if sort != 0:
            sortind = [y for (x,y) in sorted(zip(orderfield, range(len(orderfield))))]
#            pks = [pks[i] for i in sortind]
            matchfields = [matchfields[i] for i in sortind]
            pks = [y for x,y in sorted(zip(orderfield, pks))]
#            print(orderfield)
#            print(matchfields)
#            matchfields = [y for (x,y) in sorted(zip(orderfield, matchfields))]
        if limit is None:
            return pks, matchfields
        else:
            return pks[:limit], matchfields[:limit]
