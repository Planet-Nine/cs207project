from collections import defaultdict, OrderedDict
from operator import and_
from functools import reduce
import operator
import numpy as np
import gc
import uuid
from scipy.interpolate import interp1d
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

Breakpoints = {}
Breakpoints[2] = np.array([0.]) 
Breakpoints[4] = np.array([-0.67449,0,0.67449]) 
Breakpoints[8] = np.array([-1.1503,-0.67449,-0.31864,0,0.31864,0.67449,1.1503]) 
Breakpoints[16] = np.array([-1.5341,-1.1503,-0.88715,-0.67449,-0.48878,-0.31864,-0.15731,0,0.15731,0.31864,0.48878,0.67449,0.88715,1.1503,1.5341]) 
Breakpoints[32] = np.array([-1.8627,-1.5341,-1.318,-1.1503,-1.01,-0.88715,-0.77642,-0.67449,-0.57913,-0.48878,-0.40225,-0.31864,-0.2372,-0.15731,-0.078412,0,0.078412,0.15731,0.2372,0.31864,0.40225,0.48878,0.57913,0.67449,0.77642,0.88715,1.01,1.1503,1.318,1.5341,1.8627]) 
Breakpoints[64] = np.array([-2.1539,-1.8627,-1.6759,-1.5341,-1.4178,-1.318,-1.2299,-1.1503,-1.0775,-1.01,-0.94678,-0.88715,-0.83051,-0.77642,-0.72451,-0.67449,-0.6261,-0.57913,-0.53341,-0.48878,-0.4451,-0.40225,-0.36013,-0.31864,-0.27769,-0.2372,-0.1971,-0.15731,-0.11777,-0.078412,-0.039176,0,0.039176,0.078412,0.11777,0.15731,0.1971,0.2372,0.27769,0.31864,0.36013,0.40225,0.4451,0.48878,0.53341,0.57913,0.6261,0.67449,0.72451,0.77642,0.83051,0.88715,0.94678,1.01,1.0775,1.1503,1.2299,1.318,1.4178,1.5341,1.6759,1.8627,2.1539]) 

Breakpoints[128] = np.array([-2.4176,-2.1539,-1.9874,-1.8627,-1.7617,-1.6759,-1.601,-1.5341,-1.4735,-1.4178,-1.3662,-1.318,-1.2727,-1.2299,-1.1892,-1.1503,-1.1132,-1.0775,-1.0432,-1.01,-0.9779,-0.94678,-0.91656,-0.88715,-0.85848,-0.83051,-0.80317,-0.77642,-0.75022,-0.72451,-0.69928,-0.67449,-0.6501,-0.6261,-0.60245,-0.57913,-0.55613,-0.53341,-0.51097,-0.48878,-0.46683,-0.4451,-0.42358,-0.40225,-0.38111,-0.36013,-0.33931,-0.31864,-0.2981,-0.27769,-0.25739,-0.2372,-0.21711,-0.1971,-0.17717,-0.15731,-0.13751,-0.11777,-0.098072,-0.078412,-0.058783,-0.039176,-0.019584,0,0.019584,0.039176,0.058783,0.078412,0.098072,0.11777,0.13751,0.15731,0.17717,0.1971,0.21711,0.2372,0.25739,0.27769,0.2981,0.31864,0.33931,0.36013,0.38111,0.40225,0.42358,0.4451,0.46683,0.48878,0.51097,0.53341,0.55613,0.57913,0.60245,0.6261,0.6501,0.67449,0.69928,0.72451,0.75022,0.77642,0.80317,0.83051,0.85848,0.88715,0.91656,0.94678,0.9779,1.01,1.0432,1.0775,1.1132,1.1503,1.1892,1.2299,1.2727,1.318,1.3662,1.4178,1.4735,1.5341,1.601,1.6759,1.7617,1.8627,1.9874,2.1539,2.4176])
                             
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
                            self.indexes['d_vp-'+pk] = defaultdict(set)
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
                    tsarray = np.load(self.dbname+"_ts_SAX/"+pk+"_ts_SAX.npy")
                    x1 = np.linspace(min(tsarray[0,:]),max(tsarray[0,:]), self.tslen_SAX)
                    ts_SAX_data = interp1d(tsarray[0,:], ts[1,:])(x1)
                    ts_SAX_time = x1
                    ts_SAX = TimeSeries(ts_SAX_time,ts_SAX_data)
                    self.rows_SAX[pk]['ts'] = ts_SAX
                    
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
        rep = procs.isax_indb(ts_SAX,self.card,self.wordlength)
        self.SAX_tree.insert(pk, rep)
        if 'vp' in self.schema:
            self.rows_SAX[pk]['vp'] = False

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
        if pk in self.rows_SAX:
            if self.rows_SAX[pk]['vp'] == True:
                self.del_vp(pk)
            rep = procs.isax_indb(self.rows_SAX[pk]['ts'],self.card,self.wordlength)
            self.SAX_tree.delete(rep,pk)
            del self.rows_SAX[pk]
            
         
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
        if not isinstance(ts, TimeSeries):
            raise ValueError("Input must be a TimeSeries object")
        rep = procs.isax_indb(ts,self.card,self.wordlength)
        n = self.SAX_tree.search(rep)
        closestpk = None
        pkdist = None
        for pk in n.ts:
            thisdist = self.dist(ts, self.rows[pk]['ts'])
            if pkdist is None or thisdist < pkdist:
                closestpk = pk
                pkdist = thisdist
        return closestpk
        
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
        row_SAX = []
        try:
            row_SAX = self.rows_SAX[pk]
        except:
            pass
        for field in row:
            v = row[field]
            if (field in self.schema and self.schema[field]['index'] is not None) or field[:5] == 'd_vp-':
                idx = self.indexes[field]
                idx[v].add(pk)
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

class BinaryTree:
    def __init__(self, rep=None, parent=None,threshold = 10,wordlength = 16):
        self.SAX = rep
        self.parent = parent
        self.ts = []
        self.ts_SAX = []
        self.children = []
        self.th = threshold
        self.count = 0
        self.splitting_index = 0 
        self.word_length = wordlength
        self.online_mean = np.zeros(self.word_length)
        self.online_stdev = np.zeros(self.word_length)
        self.dev_accum = np.zeros(self.word_length)
        self.left = None
        self.right = None    
            
    def addLeftChild(self, rep,threshold,wordlength): 
        n = self.__class__(rep, parent=self,threshold=threshold, wordlength=wordlength)
        self.left = n
        return n
        
    def addRightChild(self, rep,threshold,wordlength):
        n = self.__class__(rep, parent=self,threshold=threshold, wordlength=wordlength)
        self.right = n
        return n
        
    def addChild(self, rep,threshold,wordlength):
        n = self.__class__(rep, parent=self,threshold=threshold, wordlength=wordlength)
        self.children += [n]
        return n
    
    def hasLeftChild(self):
        return self.left is not None

    def hasRightChild(self):
        return self.right is not None

    def hasAnyChild(self):
        return self.hasRightChild() or self.hasLeftChild()

    def hasBothChildren(self):
        return self.hasRightChild() and self.hasLeftChild()
    
    def hasNoChildren(self):
        return not self.hasRightChild() and not self.hasLeftChild()
    
    def isLeftChild(self):
        return self.parent and self.parent.left == self

    def isRightChild(self):
        return self.parent and self.parent.right == self

    def isRoot(self):
        return not self.parent

    def isLeaf(self):
        return not (self.right or self.left)
    
                
class BinarySearchTree(BinaryTree):
        
    def __init__(self, rep=None, parent=None, threshold = 10, wordlength = 16):
        super().__init__(rep, parent,threshold,wordlength)
        
    def _insert_hook(self):
        pass
            
    def insert(self, pk,rep):
        if self.isRoot():
            index = 0
            for i,symbol in enumerate(rep):
                if symbol[0] == '1':
                    index += 2**(self.word_length-i-1)
            self.children[index].insert(pk,rep)
        elif self.isLeaf() and self.count < self.th:
            self.ts += [pk]
            self.ts_SAX += [rep]
            self.mean_std_calculator(rep)
        elif self.isLeaf():
            self.ts += [pk]
            self.ts_SAX += [rep]
            self.mean_std_calculator(rep)
            self.split()
        else:
            l = len(self.left.SAX[self.splitting_index])
            if rep[self.splitting_index][l-1] == '1':
                self.right.insert(pk,rep)
            else:
                self.left.insert(pk,rep)
            
    def search(self, rep, pk = None):
        if pk is not None:
            if self.isRoot():
                index = 0
                for i,symbol in enumerate(rep):
                    if symbol[0] == '1':
                        index += 2**(self.word_length-i-1)
                self.children[index].search(rep,pk)
            elif self.isLeaf():
                if pk in self.ts:
                    return self
                else:
                    raise ValueError('"{}" not found'.format(pk))
            else:
                l = len(self.left.SAX[self.splitting_index])
                if rep[self.splitting_index][l-1] == '1':
                    self.right.search(rep,pk)
                else:
                    self.left.search(rep,pk)
        else:
            if self.isRoot():
                index = 0
                for i,symbol in enumerate(rep):
                    if symbol[0] == '1':
                        index += 2**(self.word_length-i-1)
                self.children[index].search(rep)
            elif self.isLeaf():
                return self
            else:
                l = len(self.left.SAX[self.splitting_index])
                if rep[self.splitting_index][l-1] == '1':
                    self.right.search(rep)
                else:
                    self.left.search(rep)
        
    def delete(self, rep, pk):        
        n = self.search(rep,pk)
        index = n.ts.index(pk)
        n.ts.remove(pk)
        n.ts_SAX = n.ts_SAX[:index]+n.ts_SAX[index+1:]
        n.online_mean = np.zeros(self.word_length)
        n.online_stdev = np.zeros(self.word_length)
        n.dev_accum = np.zeros(self.word_length)
        n.count = 0
        for word in n.ts_SAX:
            n.mean_std_calculator(word)
        
       
    def word2number(self,word):
        number = []
        for j in word:
            l = len(j)
            length = 2**l
            for t in sorted(Breakpoints.keys()):
                if length < t:
                    key = t
            num = int(j,2)
            ind = 2*num
            number += [Breakpoints[key][ind]]
        return np.array(number)
    
    def mean_std_calculator(self,word):
        self.count += 1
        mu_1 = self.online_mean
        value = self.word2number(word)
        delta = value - self.online_mean
        self.online_mean = self.online_mean + delta/self.count
        prod = (value-self.online_mean)*(value-mu_1)
        self.dev_accum = self.dev_accum + prod
        if self.count > 1:
            self.online_stdev = np.sqrt(self.dev_accum/(self.count-1))
    
    def getBreakPoint(self,s):
        l = len(s)
        length = 2**l
        for t in sorted(Breakpoints.keys()):
            if length < t:
                key = t
        num = int(s,2)
        ind = 2*num
        return Breakpoints[key][ind]
    
    def split(self):
        segmentToSplit = None
        if self.SAX is not None:
            for i,s in enumerate(self.SAX):
                b = self.getBreakPoint(s)         
                diff = None
                if b <= self.online_mean[i] + 3*self.online_stdev[i] and b >= self.online_mean[i] - 3*self.online_stdev[i]
                    if diff is None or np.abs(self.online_mean[i] - b) < diff:
                        segmentToSplit = i
                        diff = np.abs(self.online_mean[i] - b)
                    
            self.IncreaseCardinality(i)
    
    def IncreaseCardinality(self, segment):
        if self.SAX is None:
            raise ValueError('Cannot increase cardinality of root node')
        newSAXupper = self.SAX
        newSAXupper[segment] = newSAXupper[segment]+'1'
        newSAXlower = self.SAX
        newSAXlower[segment] = newSAXlower[segment]+'0'
        newtsuppper = []
        newtslower = []
        newts_SAXupper = []
        newts_SAXlower = []
        l = len(newSAXupper[segment])
        for i,word in enumerate(self.ts_SAX):
            if word[segment][l-1] == '1':
                newts_SAXupper += [word]
                newtsupper += [self.ts[i]]
            else:
                newts_SAXlower += [word]
                newtslower += [self.ts[i]]
        
        self.addLeftChild(rep=newSAXlower,threshold=self.th, wordlength=self.word_length)
        self.addRightChild(rep=newSAXupper,threshold=self.th, wordlength=self.word_length)
        for word in newts_SAXupper:
            self.right.mean_std_calculator(word)
        self.right.ts = newtsuppper
        self.right.ts_SAX = newts_SAXupper
        for word in newts_SAXlower:
            self.left.mean_std_calculator(word)
        self.left.ts = newtslower
        self.left.ts_SAX = newts_SAXlower
        self.ts = []
        self.ts_SAX = []
        self.count = 0
        self.online_mean = None
        self.online_stdev = None
        self.dev_accum = None
        self.splitting_index = segment
        
        
    def __iter__(self):
        if self is not None:
            if self.hasLeftChild():
                for node in self.left:
                    yield node
            for _ in range(self.count):
                yield self
            if self.hasRightChild():
                for node in self.right:
                    yield node
                    
    def __len__(self):#expensive O(n) version
        start=0
        for node in self:
            start += 1
        return start
    
    def __getitem__(self, i):
        return self.ithorder(i+1)
    
    def __contains__(self, data):
        return self.search(data) is not None

class Tree_Initializer():
    def __init__(self, threshold = 10, wordlength = 16):
        self.tree = BinarySearchTree(threshold=threshold, wordlength=wordlength)
        words = [list('{0:b}'.format(i).zfill(int(np.log(2**wordlength-1)/np.log(2))+1)) for i in range(2**wordlength)]
        for i in range(2**wordlength):
            self.tree.addChild(words[i],threshold,wordlength)
        
    
