# CS 207 Project Fall 2016

[![Build Status](https://travis-ci.org/Planet-Nine/cs207project.svg?branch=master)](https://travis-ci.org/Planet-Nine/cs207project)
[![Coverage Status](https://coveralls.io/repos/github/Planet-Nine/cs207project/badge.svg?branch=master)](https://coveralls.io/github/Planet-Nine/cs207project?branch=master)

## Overview

This package allows for the persistent storage of TimeSeries objects in a database, as well as efficient similarity searches within that database using both a cross-correlation-based vantage point search as well as a SAX-tree-based search.  The database may be accessed directly through Python scripts, or by the mediation of a client/server setup including REST API support.

##### Persistence Architecture

The state of the database is saved to file each time a change is made.  The base filename (dbname) is specified when the PersistentDB object is initialized.  The architecture of the persistence is very straightforward, falling into three major (sets of) files:
- dbname_ts/pk_ts.npy: The directory dbname_ts saves each of the timeseries objects as a numpy array with dimensions 2 x N, where the first row is the time and the second row is the data.  The TimeSeries object is reinitialized upon load using this information.  These files are stored and named according to the primary key when they are first inserted into the database.
- dbname_ts_SAX/pk_ts-SAX.npy: Same as above, but the SAX representation of each TimeSeries rather than the raw data.
- dbname: An ASCII file which is a transaction log, consisting of a series of lines of the form pk:field:val where pk = primary key, field = fieldname, and val = value.  A new line is added to the log every time a change is made to the database.  The state of the database may be reconstructed based on this log if the local copy is closed.  If an object is deleted from the database, the line will be of the form pk:DELETE:val, and upon load the database will disregard any entries with that primary key prior to the deletion entry.

##### Additional Feature

The iSAX tree (a SAXTree object) is set up with the following arguments:
- rep : list of strings, which is the iSAX representation of the node (the root node has no iSAX representation)
- parent : SAXTree object, which is the parent of the given node 
- threshold : integer, which is the maximum number of time series that a leaf node can store (default is 10) 
- wordlength : integer, which is a multiple of 2 and is the number of symbols contained in an iSAX representation or the number of horizontal slices into which the time series are to be divided.

It further has the attributes:
- parent : which is simply parent from above
- SAX : which is simply the rep from above
- ts : a list containing the primary keys of the time series objects (it is nonempty only for leaf nodes)
- ts_SAX : a list containing the iSAX representations of the time series objects (it is nonempty only for leaf nodes)
- children : a list containing the SAXTree objects which are the children of the given node, it is nonempty only for the root node which has more then two children, the other nodes have only possibly right and left children 
- th : which is simply threshold from above
- count : the number of time series contained in a leaf node (for nodes that are not leaf nodes count is zero)
- splitting_index : for internal nodes this is the index, or segment of the iSAX representation that is split in creating the two children nodes, leaf nodes and the root node have None as their splitting index  
- word_length : is simply wordlength from above
- online_mean : a numpy array containing the online means computed for each of the word_length segments from the time series in the node (nonzeros only for leaf nodes) 
- online_stdev : a numpy array containing the online standard deviations computed for each of the word_length segments from the time series in the node (nonzeros only for leaf nodes) 
- dev_accum : a numpy array of intermediate quantities in the calculation of the online standard deviation
- left : left child node (SAXTree object) of given node  (None for leaf nodes and for root)
- right : right child node (SAXTree object) of given node (None for leaf nodes and for root)

To perform a iSAX search run both_SAX.sh. The prompts are relatively informative, but here are some additional hints. You should first finish with all the windows stemming from the window that starts with, "This is a brief introduction to the similarity search for time series", before moving on to the other window. Only load data from an existing database if one exists and the files should be ascii for the database and .npy for the time series. If unsure of how to format the files, run the code first without requesting to input a time series and after having the program generate some time series automatically, check the formatting of the files. A note on the threshold, it is effectively the maximum number of time series that will be returned by an internal search and that distances will be computed from. An important note is that it is possible for the search for a closest matching time series to return None. This is because the root node initially gets populated with 2^(word length) children representing all permutations of '1' and '0' among the (word length) symbols in the SAX representation. These children are not all filled with time series initially, so it is possible that a search stumbles on a node that does not actually have any time series associated with it, hence the None value returned. One solution is to input a number of time series at least a few times 2^(word length), a rough estimate of the appropriate number of time series would be order of the threshold times 2^(word length) (though 4 times usually works). Another important note is there is the potential for overflow of the SAX tree. The max depth of a search is equal to the logarithm of the cardinality with base 2. So if you input a cardinality of 64 the max depth of the tree is 6. This means that if there are too many time series inserted into the database the nodes may need to split more than 6 times causing overflow. Ways to remedy this situation are by increasing the cardinality or the threshold. 

##### REST API

The REST API provides the a way to interact with the database via HTML. This is implemented with tornado in an asynchronous manner. Both the listening for HTML commands and querying the database is implemented in one event loop. Seven operations are currently supported:

1. Inserting time series
2. Adding vantage point
3. Deleting time series
4. Upsert meta information
5. Select
6. Augmented select
7. Similarity Search


## Installation/Usage

##### Database population and searching

The database is initialized by the creation of a new PersistentDB object, which takes the following arguments:
- schema : dict, as specified below
- pkfield : str, primary key field which must match a fieldname in the schema
- load : bool (optional, default=False), whether to load a database from an existing file 
- dbname : str (optional, default="db"), the filename where the database will be stored and/or loaded
- overwrite : bool (optional, default=False), whether to overwrite any existing database of the same name, ignored if load == True
- dist : function (optional, default=cross-correlation), calculates the distance between two TimeSeries objects ts1 and ts2, used for vantage point calculations, must take arguments (ts1, ts2)

If load = False and overwrite = False and there is an existing database with dbname, the initialization will raise an error.

The input schema should be a dictionary with the following form:

```schema = { 'fieldname1' : {'type': type, 'index' : N},
  	       'fieldname2' : {'type': type, 'index' : N} ... }```
  	       
Requirements for schema:
- No fieldname may begin with the string 'd_vp-'.
- No fieldname may contain the ':' character.
- The fieldname 'DELETE' is forbidden.
- Every field with the exception of 'ts' must specify a type.
- The only types which are supported are int, float, bool, and str.
- The field specified as the primary key must have 'type':str
- For consistency, it would make sense if 'ts' were a field in the schema, but the database will function even if it is not.
- If the 'index' property has any value other than None, that field will be searchable in the select() function.
- If vantage points are desired, 'vp' : {'type':bool} must appear in the schema.  Otherwise, the add_vp function will raise an error when called.
- The 'additional' options in the select() function will not work unless 'order' appears in the schema.

The database contains two methods for similarity search: 
- simsearch(ts): Uses cross-correlation to define the similarity of two TimeSeries objects.  Note: you must add at least one vantage point with the add_vp() method before using this function.
- simsearch_SAX(ts): Uses a SAX representation to define the similarity of two TimeSeries objects.

Both methods will return the primary key of the object in the database with the shortest distance to the input TimeSeries, as defined by that method.

Further details on the functionality of the PersistentDB class may be found in tsdb/persistentdb_readme.txt

##### Running the server

An example server can be found in go_server.py.  The important line to note here is

``` db = PersistentDB(schema, 'pk', overwrite=True) ```

This will create a new database named "db", overwriting any existing database of that name.  To load a database, set load=True, and if you would like the database to have a different name, set dbname="whateveryoulike".

You may also change the schema in go_server.py, though note that it must conform to the requirements listed above.

##### REST API

Instructions for running API

1. Run the server at port 9999(example: go_server.py).
2. Run the REST API web client(rest_api.py). 
3. Paste HTML commands to REST API web client (current port 8080) as needed. (example: go_rest.py)


