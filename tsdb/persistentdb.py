from collections import defaultdict, OrderedDict
from operator import and_
from functools import reduce
import operator
import numpy as np
import sys
sys.path.insert(0, '../')   # This is sketchy AF but I'm not sure how else to do it
from timeseries import TimeSeries
import os
import procs
import random

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

    def __init__(self, schema, pkfield, load=False, dbname="db", overwrite=False, dist=procs.corr_indb):
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
                if field[:5] == 'd_vp-':
                    raise ValueError("Field names beginning with 'd_vp-' are forbidden")
                if field == 'vp' and schema[field]['type'] != bool:
                    raise ValueError("Field 'vp' must be of boolean type")
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
        self.dist = dist
        self.vps = []
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
                            if self.schema[field]['type'] == bool:
                                if val == 'False': 
                                    self.rows[pk][field] = False
                                else:
                                    self.rows[pk][field] = True
                            else:
                                self.rows[pk][field] = self.schema[field]['type'](val)
                        if field == 'vp' and val == 'True':
                            self.vps.append(pk)
                            self.indexes['d_vp-'+pk] = defaultdict(set)
                    elif field == 'DELETE':
                        if 'vp' in schema and self.rows[pk]['vp'] == True:
                            self.del_vp(pk)
                        del self.rows[pk]
                    elif field[:5] == 'd_vp-':
                        self.rows[pk][field] = float(val)
                    else:
                        raise IOError("Database is incompatible with input schema")
                fd.close()
                
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
        if 'vp' in self.schema:
            fd.write(pk+':vp:False\n')
        fd.close()

        self.rows[pk]['ts'] = ts  
        if 'vp' in self.schema:
            self.rows[pk]['vp'] = False

        for vp in self.vps:
            ts1 = self.rows[vp]['ts']
            self.upsert_meta(pk, {'d_vp-'+vp : self.dist(ts1,ts)})

        self.update_indices(pk)

    def delete_ts(self, pk):
        if pk in self.rows:
            if self.rows[pk]['vp'] == True:
                self.del_vp(pk)
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
            if field in self.schema:
                try:
                    convertedval = self.schema[field]['type'](meta[field])
                    if self.schema[field]['type'] == str and ':' in convertedval:
                        raise ValueError("Strings may not include the ':' character") 
                    self.rows[pk][field] = meta[field]
                except:
                    raise ValueError("Value not compatible with type specified in schema")
            elif field[:5] == 'd_vp-':
                self.rows[pk][field] = float(meta[field])
            else:
                raise ValueError('Field not supported by schema')


        fd = open(self.dbname, 'a')
        for field in meta:
            fd.write(pk+':'+field+':'+str(meta[field])+'\n')
        fd.close()

        self.update_indices(pk)

    def add_vp(self, pk=None, dist=procs.corr_indb):
        """
        Adds pk as a vantage point

        Parameters
        ----------
        pk : str or None
            The primary key of the timeseries which is to be added as a vantage point.
            If None, method will choose a random entry in the database.
        dist : function
            The function used to compute distances between timeseries objects.
            Must have arguments (ts1, ts2).
        """

        # ---- Validating input ---- #
        if 'vp' not in self.schema:
            raise ValueError("Vantage points not supported by schema, must include 'vp' field")
        if pk is None:
            pkrand = self.rows.keys()
            random.shuffle(list(pkrand))
            foundvp = False
            for k in pkrand:
                if self.rows[k]['vp'] == False:
                    pk = k
                    foundvp = True
                    break
            if foundvp == False:
                raise ValueError("No more primary keys available as vantage points")
        elif pk not in self.rows:
            raise ValueError("Primary key not in database")
        elif self.rows[pk]['vp']:
            raise ValueError("This timeseries is already a vantage point")
        if self.dist is None:
            self.dist = dist
        elif self.dist != dist:
            raise ValueError("All vantage points must follow same distance calculation")
        
        self.vps.append(pk)
        self.upsert_meta(pk, {'vp':True})
        ts1 = self.rows[pk]['ts']
        for key in self.rows:
            ts2 = self.rows[key]['ts']
            self.upsert_meta(key, {'d_vp-'+pk:self.dist(ts1,ts2)})
        
    def del_vp(self, vp):
        """ Removes the d_vp-vp field from all rows """
        for pk in self.rows:
            del self.rows[pk]['d_vp-'+vp]
        self.vps.remove(vp)
        del self.indexes['d_vp-'+vp]

    def simsearch(self, ts):
        """ Search over all timeseries in the database and return the primary key 
          of the object which is closest """
        if not isinstance(ts, TimeSeries):
            raise ValueError("Input must be a TimeSeries object")
        if len(self.vps) == 0:
            raise ValueError("Database must contain vantage points before simsearch can be called")

        # Find closest vantage point
        closestvp = None
        vpdist = None
        for vp in self.vps:
            thisdist = self.dist(ts, self.rows[vp]['ts'])
            if vpdist is None or thisdist < vpdist:
                closestvp = vp
                vpdist = thisdist
        
        # Select all timeseries within 2*vpdist from closestvp
        closepks,_ = self.select(meta={'d_vp-'+closestvp:{'<=':2*vpdist}}, fields=None)

        # Find closest timeseries
        closestpk = None
        pkdist = None
        for pk in closepks:
            thisdist = self.dist(ts, self.rows[pk]['ts'])
            if pkdist is None or thisdist < pkdist:
                closestpk = pk
                pkdist = thisdist

        return closestpk

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
            if (field in self.schema and self.schema[field]['index'] is not None) or field[:5] == 'd_vp-':
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
                if isinstance(meta[field],dict):
                    for opkey in meta[field]:
                        op = OPMAP[opkey]
                        compval = meta[field][opkey]
                else:
                    op = OPMAP['==']
                    compval = meta[field]

                pks_field = []
                if field not in self.schema and field[:5] != 'd_vp-':
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
