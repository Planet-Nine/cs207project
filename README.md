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

RODRICK TO-DO: Describe setup of SAX tree

##### REST API

The REST API provides the a way to interact with the database via HTML. This is implemented with tornado in an asynchronous manner. Both the listening for HTML commands and querying the database is implemented in one event loop. Seven operations are currently supported. 
1. Inserting time series
2. Adding vantage point
3. Deleting time series
4. Upsert meta information,
5. Select
6. Augmented select
7. Similarity Search


## Installation/Usage

##### Database population and searching

The database is initialized by the creation of a new PersistentDB object, which takes the following arguments:
- schema : dict, as specified above
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

TO-DO: Describe general setup.

##### REST API

Instructions for running api

1. Run the server at port 9999(example: go_server.py).
2. Run the REST API web client(rest_api.py). 
3. Past HTML commands to REST API web client as needed. (example: go_rest.py)


