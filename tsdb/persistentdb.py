from collections import defaultdict, OrderedDict
from operator import and_
from functools import reduce
import operator
import numpy as np
import sys
sys.path.insert(0, '../')   # This is sketchy AF but I'm not sure how else to do it
from timeseries import TimeSeries
import os

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


class PersistentDB:
    "Database implementation with a local dictionary, which saves all necessary data to files for later use"
    def __init__(self, schema, pkfield, load=False, dbname="db", overwrite=False):
        """
        Parameters
        ----------
        schema : dict
            Key = name of field (e.g. 'ts', 'mean')
            Value = dict of that field's properties.  Recognized keys include:
                'type': Required for all fields except ts.  pkfield must have type str.
                'index': Required for all fields.  
        pkfield : str
            The name of the field which will be the primary key.  Must match a key in schema.
        load : bool
            Whether to populate the database with an existing one on file.
        dbname : str
            Database filename
        overwrite : bool
            If load=False, whether to overwrite an existing database.

        Attributes
        ----------
        indexes : dict
        rows : dict
            Key = primary key
            Value = dict of the fields associated with each key
        schema : dict (See above)
        pkfield : str (See above)
        dbname : str (See above)
        tslen : int
            The length of each timeseries in the database, strictly enforced
        """
        # ---- Validating input ---- #
        if not isinstance(pkfield, str):
            raise ValueError("Field name must be of type str")
        if not isinstance(load, bool):
            raise ValueError("Load must be of type bool")
        if not isinstance(dbname, str):
            raise ValueError("Database name must be string")
        if not isinstance(overwrite, bool):
            raise ValueError("Overwrite must be of type bool")
        if isinstance(schema, dict):
            for field in schema:
                if field != 'ts':   
                    if 'type' not in schema[field]:
                        raise ValueError("Schema must specify type for each non-ts field")
                    if field == pkfield and schema[field]['type'] != str:
                        raise ValueError("Primary key must be of type str")
                    if schema[field]['type'] not in [int, float, bool, str]:
                        raise ValueError("Only types int, float, bool, and str are supported")
        else:
            raise ValueError("Schema must be a dictionary")
        if pkfield not in schema:
            raise ValueError("Primary key field must be included in schema")

        # Assign attributes according to schema
        self.indexes = {}
        self.rows = {}
        self.schema = schema
        self.dbname = dbname
        self.pkfield = pkfield
        self.tslen = None
        self.overwrite = overwrite
        for s in schema:
            indexinfo = schema[s]['index']
            # convert = schema[s]['type']
            # later use binary search trees for highcard/numeric
            # bitmaps for lowcard/str_or_factor
            if indexinfo is not None:
                self.indexes[s] = defaultdict(set)

        # Trees for any numerical fields for faster searching?  (Do later...)

        if load:
            try:
                fd = open(dbname)
                for l in fd.readlines():
                    [pk, field, val] = l.strip().split(":")
                    if field in self.schema:
                        if pk not in self.rows:
                            self.rows[pk] = {pkfield:pk}
                        else:
                            self.rows[pk][field] = self.schema[field]['type'](val)
                    elif field == 'DELETE':
                        del self.rows[pk]
                    else:
                        raise IOError("Database is incompatible with input schema")
                fd.close()
                
                # Separate vantage point file?
                # Will need to add something here for vantage points

                # Read in timeseries of non-deleted keys
                for pk in self.rows:
                    tsarray = np.load(self.dbname+"_ts/"+pk+"_ts.npy")
                    self.rows[pk]['ts'] = TimeSeries(tsarray[0,:], tsarray[1,:])
                    self.tslen = tsarray.shape[1]

                self.index_bulk(list(self.rows.keys()))
            except:
                raise IOError("Database does not exist or has been corrupted")
        else:
            if os.path.exists(dbname) and overwrite == False:
                raise ValueError("Database of that name already exists. Delete existing db, rename, or set overwrite=True.")

    def insert_ts(self, pk, ts):    
        try:
            pk = str(pk)
        except:
            raise ValueError("Primary keys must be string-compatible")
        if ':' in pk:
            raise ValueError("Primary keys may not include the ':' character") 
        if not isinstance(ts, TimeSeries):
            raise ValueError('Must insert a TimeSeries object')

        if pk not in self.rows:
            self.rows[pk] = {self.pkfield:pk}
        else:
            raise ValueError('Duplicate primary key found during insert')

        # Save timeseries as a 2d numpy array
        if self.tslen is None:
            self.tslen = len(ts)
        elif len(ts) != self.tslen:
            raise ValueError('All timeseries must be of same length')
        if not os.path.exists(self.dbname+"_ts"):
            os.makedirs(self.dbname+"_ts")
        np.save(self.dbname+"_ts/"+pk+"_ts.npy", np.vstack((ts.time, ts.data)))

        # Save a record in the database file
        if self.overwrite or not os.path.exists(self.dbname):
            fd = open(self.dbname, 'w')
            self.overwrite = False
        else:
            fd = open(self.dbname, 'a')
        fd.write(pk+':'+self.pkfield+':'+pk+'\n')
        fd.close()

        self.rows[pk]['ts'] = ts  
        self.update_indices(pk)

    def delete_ts(self, pk):
        if pk in self.rows:
            del self.rows[pk]
            fd = open(self.dbname, 'a')
            fd.write(pk+':DELETE:0\n')
            fd.close()

    def upsert_meta(self, pk, meta):
        if isinstance(meta, dict) == False:
            raise ValueError('Metadata should be in the form of a dictionary')
        if pk not in self.rows:
            raise ValueError('Timeseries should be added prior to metadata')
        for field in meta:
            if field not in self.schema:
                raise ValueError('Field not supported by schema')
            try:
                convertedval = self.schema[field]['type'](meta[field])
                if self.schema[field]['type'] == str and ':' in convertedval:
                    raise ValueError("Strings may not include the ':' character") 
                self.rows[pk][field] = meta[field]
            except:
                raise ValueError("Value not compatible with type specified in schema")

        fd = open(self.dbname, 'a')
        for field in meta:
            fd.write(pk+':'+field+':'+str(meta[field])+'\n')
        fd.close()

        # Might need to do something about vantage points here

        self.update_indices(pk)

    def index_bulk(self, pks=[]):
        if len(pks) == 0:
            pks = self.rows
        for pkid in pks:
            self.update_indices(pkid)

    # Update to tree structure
    def update_indices(self, pk):
        row = self.rows[pk]
        for field in row:
            v = row[field]
            if self.schema[field]['index'] is not None:
                idx = self.indexes[field]
                idx[v].add(pk)

    def select(self, meta, fields, additional=None):

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
            matchfields = [matchfields[i] for i in sortind]
            pks = [y for x,y in sorted(zip(orderfield, pks))]
        if limit is None:
            return pks, matchfields
        else:
            return pks[:limit], matchfields[:limit]
