from collections import defaultdict, OrderedDict
from operator import and_
from functools import reduce
import operator
import numpy as np
import gc
import uuid
from scipy.interpolate import interp1d
import sys
sys.path.insert(0, '../')  
from timeseries import TimeSeries
import os
import procs
from procs.isax import isax_indb
import random
from .trees import BinarySearchTree, Tree_Initializer

OPMAP = {
    '<': operator.lt,
    '>': operator.gt,
    '==': operator.eq,
    '!=': operator.ne,
    '<=': operator.le,
    '>=': operator.ge
}
                             
class PersistentDB:
    "Database implementation with a local dictionary, which saves all necessary data to files for later use"

    def __init__(self, schema, pkfield, load=False, dbname="db", overwrite=False, dist=procs.corr_indb, threshold = 10, wordlength = 16, tslen = 256, cardinality = 64):
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
        dist : function
            Calculates the distance between two TimeSeries objects, must take arguments (ts1, ts2)

        Attributes
        ----------
        indexes : dict
            Key = fieldname
            Value = binary search tree (if int or float) or dictionary of sets (otherwise) mapping values to pks
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
        if not isinstance(threshold, int):
            raise ValueError("Threshold must be of type int")
        if not isinstance(wordlength, int):
            raise ValueError("Word length must be of type int")
        if threshold <= 0:
            raise ValueError("Threshold must be greater than zero")
        if wordlength <= 0:
            raise ValueError("Word length must be greater than zero")
        if '{0:b}'.format(wordlength)[-1] != '0':
            raise ValueError("Word length must be a power of two")
        if not isinstance(tslen, int):
            raise ValueError("TimeSeries length must be of type int")
        if tslen < wordlength:
            raise ValueError("TimeSeries length must be greater than or equal to the word length")
        if '{0:b}'.format(tslen)[-1] != '0':
            raise ValueError("TimeSeries length must be a power of two")
        if not isinstance(cardinality, int):
            raise ValueError("Cardinality must be of type int")
        if cardinality <= 0:
            raise ValueError("Cardinality must be greater than zero")
        if '{0:b}'.format(cardinality)[-1] != '0':
            raise ValueError("Cardinality must be a power of two")
        if cardinality > 64:
            raise ValueError("Cardinalities greater than 64 are not supported")    
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
        self.rows_SAX = {}
        self.wordlength = wordlength
        self.threshold = threshold
        self.SAX_tree = Tree_Initializer(threshold = threshold, wordlength = wordlength).tree    
        self.card = cardinality
        self.schema = schema
        self.dbname = dbname
        self.pkfield = pkfield
        self.tslen = None
        self.tslen_SAX = tslen
        self.overwrite = overwrite
        self.dist = dist
        self.vps = []
        for s in schema:
            indexinfo = schema[s]['index']
            if indexinfo is not None:
                if schema[s]['type'] == int or schema[s]['type'] == float:
                    self.indexes[s] = BinarySearchTree()
                else:  # Add a bitmask option for strings?
                    self.indexes[s] = defaultdict(set)

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
                        if pk not in self.rows_SAX:
                            self.rows_SAX[pk] = {pkfield:pk}
                        else:
                            if self.schema[field]['type'] == bool:
                                if val == 'False': 
                                    self.rows_SAX[pk][field] = False
                                else:
                                    self.rows_SAX[pk][field] = True
                            else:
                                self.rows_SAX[pk][field] = self.schema[field]['type'](val)
                        if field == 'vp' and val == 'True':
                            self.vps.append(pk)
                            self.indexes['d_vp-'+pk] = BinarySearchTree()
                    elif field == 'DELETE':
                        if 'vp' in schema and self.rows[pk]['vp'] == True:
                            self.del_vp(pk)
                        del self.rows[pk]
                        del self.rows_SAX[pk]
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
                    tsarray2 = np.load(self.dbname+"_ts_SAX/"+pk+"_ts_SAX.npy")
                    x1 = np.linspace(min(tsarray2[0,:]),max(tsarray2[0,:]), self.tslen_SAX)
                    ts_SAX_data = interp1d(tsarray2[0,:], tsarray2[1,:])(x1)
                    ts_SAX_time = x1
                    ts_SAX = TimeSeries(ts_SAX_time,ts_SAX_data)
                    self.rows_SAX[pk]['ts'] = ts_SAX
                    rep = isax_indb(ts_SAX,self.card,self.wordlength)
                    self.SAX_tree.insert(pk, rep)
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
        if pk not in self.rows_SAX:
            self.rows_SAX[pk] = {self.pkfield:pk}
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
        
        x1 = np.linspace(min(ts.time),max(ts.time), self.tslen_SAX)
        ts_SAX_data = interp1d(ts.time, ts.data)(x1)
        ts_SAX_time = x1
        ts_SAX = TimeSeries(ts_SAX_time,ts_SAX_data)
        if not os.path.exists(self.dbname+"_ts_SAX"):
            os.makedirs(self.dbname+"_ts_SAX")
        np.save(self.dbname+"_ts_SAX/"+pk+"_ts_SAX.npy", np.vstack((ts_SAX.time, ts_SAX.data)))

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

        self.rows_SAX[pk]['ts'] = ts_SAX  
        rep = isax_indb(ts_SAX,self.card,self.wordlength)
        self.SAX_tree.insert(pk, rep)
        if 'vp' in self.schema:
            self.rows_SAX[pk]['vp'] = False

        for vp in self.vps:
            ts1 = self.rows[vp]['ts']
            self.upsert_meta(pk, {'d_vp-'+vp : self.dist(ts1,ts)})

        self.update_indices(pk)


    def delete_ts(self, pk):    # Delete values from trees
        if pk in self.rows:
            for field in self.rows[pk]:
                if self.schema[field]['index'] is not None:
                    if self.schema[field]['type'] in [int, float]:
                        self.indexes[field].delete(self.rows[pk][field], pk)
                    else:
                        self.indexes[field][self.rows[pk][field]].remove(pk)  
                if field == 'vp' and self.rows[pk]['vp'] == True:
                    self.del_vp(pk)
            del self.rows[pk]
            fd = open(self.dbname, 'a')
            fd.write(pk+':DELETE:0\n')
            fd.close()
        if pk in self.rows_SAX:
            if self.rows_SAX[pk]['vp'] == True:
                self.del_vp(pk)
            rep = isax_indb(self.rows_SAX[pk]['ts'],self.card,self.wordlength)
            self.SAX_tree.delete(rep,pk)
            del self.rows_SAX[pk]
            
    def upsert_meta(self, pk, meta):
        if isinstance(meta, dict) == False:
            raise ValueError('Metadata should be in the form of a dictionary')
        if pk not in self.rows:
            raise ValueError('Timeseries should be added prior to metadata')
        oldrow = self.rows[pk].copy()
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

        self.update_indices(pk, oldrow)

    def add_vp(self, pk=None):
        """
        Adds pk as a vantage point

        Parameters
        ----------
        pk : str or None
            The primary key of the timeseries which is to be added as a vantage point.
            If None, method will choose a random entry in the database.
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
        
        self.vps.append(pk)
        self.upsert_meta(pk, {'vp':True})
        self.indexes['d_vp-'+pk] = BinarySearchTree()
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

    def simsearch_SAX(self, ts):
        if isinstance(ts,TimeSeries):
            ts = [ts.time,ts.data]
        x1 = np.linspace(min(ts[0]),max(ts[0]), self.tslen_SAX)
        ts_SAX_data = interp1d(ts[0], ts[1])(x1)
        ts_SAX_time = x1
        ts_SAX = TimeSeries(ts_SAX_time,ts_SAX_data)
        rep = isax_indb(ts_SAX,self.card,self.wordlength)
        n = self.SAX_tree.search(rep)
        closestpk = None
        pkdist = None
        for pk in n.ts:
            thisdist = self.dist(ts_SAX, self.rows_SAX[pk]['ts'])
            if pkdist is None or thisdist < pkdist:
                closestpk = pk
                pkdist = thisdist

        return closestpk
        # if closestpk:
        #     return self.rows[closestpk]['ts']
        # else:
        #     return None
        
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
         
    def update_indices(self, pk, oldrow=None):
        # If oldrow = None, assume all assignments are new.  If not, check whether the old values need to be deleted.
        row = self.rows[pk]
        row_SAX = []
        try:
            row_SAX = self.rows_SAX[pk]
        except:
            pass
        for field in row:
            val = row[field]
            if field[:5] == 'd_vp-' or (self.schema[field]['index'] is not None and self.schema[field]['type'] in [int,float]):
                if oldrow is not None and field in oldrow:
                    if oldrow[field] != val:
                        oldval = oldrow[field]
                        self.indexes[field].delete(oldval, pk)
                        self.indexes[field].put(val, pk)
                else:
                    self.indexes[field].put(val, pk)
            elif self.schema[field]['index'] is not None:
                if oldrow is not None and field in oldrow:
                    if oldrow[field] != val:
                        oldval = oldrow[field]
                        self.indexes[field][oldval].remove(pk)
                        self.indexes[field][val].add(pk)
                else:
                    self.indexes[field][val].add(pk)
        for field in row_SAX:
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
        elif len(meta) == 1 and self.pkfield in meta:
            pks = [meta['pk']]
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
                if field[:5] != 'd_vp-' and self.schema[field]['index'] is None:
                    raise ValueError('May only search by indexed fields or primary key')
                else:
                    if field[:5] == 'd_vp-' or self.schema[field]['type'] in [int, float]:
                        if op == OPMAP['==']:
                            pks_field = self.indexes[field].get(compval)
                        else:
                            pks_field = self.indexes[field].collect(compval, op)
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
