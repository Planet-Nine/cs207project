The PersistentDB class creates a database of timeseries objects and metadata about those objects.

The input schema should be a dictionary with the following form:
    schema = { 'fieldname1' : {'type': type, 'index' : N},
    	       'fieldname2' : {'type': type, 'index' : N} ... }
Requirements for schema:
     - No fieldname may begin with the string 'd_vp-'
     - No fieldname may contain the ':' character
     - The field specified as the primary key must have 'type':str
     - For consistency, it would make sense if 'ts' were a field in the schema, but the database will 
       function even if it is not.
     - 'ts' is the only field that is not required to have a type specified.
     - The only types which are supported are int, float, bool, and str.
     - If vantage points are desired, 'vp' : {'type':bool} must appear in the schema.  Otherwise, the
       add_vp function will raise an error when called.
     - The 'additional' options in the select() function will not work unless 'order' appears in the schema.

Functions:
    PersistentDB(schema, pkfield, load=False, dbname="db", overwrite=False, dist=procs.corr_indb)
        schema : dict, as specified above
	pkfield : str, primary key field which must match a fieldname in the schema
	load : bool, whether to load a database from an existing file 
	dbname : str, the filename where the database will be stored
	overwrite : bool, whether to overwrite any existing database of the same name
	                  not used if load == True
	dist : function, calculates the distance between two TimeSeries objects ts1 and ts2
	                 used for vantage point calculations, must take arguments (ts1, ts2)

    insert_ts(pk, ts)
    Inserts a TimeSeries object into the database for the first time, and calculates the distances 
    to any existing vantage points.
        pk : str, the primary key to be associated with this TimeSeries
                  may not be already in use, or contain the ':' character
	ts : TimeSeries, the object to be inserted into the database
	                 must have the same length as any previously-inserted object

    delete_ts(pk)
    Deletes the entry with primary key pk from the database.  If the object is a vantage point,
    also deletes all distances to the object.

    upsert_meta(pk, meta)
    Store metadata related to the object with primary key pk.
        meta : dict, with form {'field1': val1, 'field2':val2, etc.}
	             all fieldnames must be specified in schema
		     all values must be castable to the type specified in the schema

    add_vp(pk=None)
    Adds a vantage point to the database
        pk : str or None, the primary key of the object which is to be a vantage point
	                  if pk == None, will choose a random object from the database

    simsearch(ts)
    Searches the database for the object which is most similar to ts
        ts : TimeSeries, must have same length as objects in database
    Return value : str, the primary key of the most-similar object in the database

    select(meta, fields, additional=None)
    Finds the objects in the database with the desired characteristics.    
        meta : dict, search parameters
	             must have structure {'field1':{OP:val1}, 'field2':val2, etc.},
		         where OP is an operator in the OPMAP such as '<='
		     if empty, will return all entries in database
	fields : list or None, designates which fields to return if any
	                       if fields == None, will return no fields
			       if fields is an empty list, will return all fields
	additional : dict, allows for sorting or a reduced number of return values
	    	     	   two fields recognized: 'sort_by' and 'limit'
			   if 'sort_by':'+order', sorts the results by the 'order' field
			   if 'sort_by':'-order', reverses the order
			   if 'limit' is specified, will only return up to the number of entries specified
			   this argument is meaningless if 'order' does not appear in the schema
    Return value : (pks, returnfields), where pks is a list of primary keys and
    	   	   	 		returnfields is a corresponding list of dictionaries of their fields 
			       
