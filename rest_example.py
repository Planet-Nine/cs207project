import urllib
import timeseries as ts
import numpy as np 
import requests
testts=ts.TimeSeries([1,2,3],[4,5,6])
requests.post("http://localhost:8080/insert_ts",params=urllib.parse.urlencode({'pk':'pk3','ts':testts.to_json()}))
meta_dict={'order': 1, 'blarg': 2}
requests.post("http://localhost:8080/upsert_meta",params=urllib.parse.urlencode({'pk':'pk1','meta_dict':meta_dict}))
requests.get("http://localhost:8080/select",params=urllib.parse.urlencode({'meta_dict':meta_dict}))
requests.get("http://localhost:8080/augmented_select",params=urllib.parse.urlencode({'proc':'corr','target':['distance'],'arg' : testts.to_json()}))