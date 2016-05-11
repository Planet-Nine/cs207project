import asyncio
from .tsdb_serialization import serialize, LENGTH_FIELD_LENGTH, Deserializer
from .tsdb_ops import *
from .tsdb_error import *

class TSDBrestapi(object):
    "client"
    def __init__(self, port=9999):
        self.port = port

    def insert_ts(self, primary_key, ts):
        #your code here, construct from the code in tsdb_ops.py
        msg = TSDBOp_InsertTS(primary_key, ts).to_json()
        return self._send(msg)

    def upsert_meta(self, primary_key, metadata_dict):
        msg = TSDBOp_UpsertMeta(primary_key, metadata_dict).to_json()
        return self._send(msg)
        
    def delete_ts(self, primary_key):
        msg = TSDBOp_DeleteTS(primary_key).to_json()
        print("C> delete_ts msg", msg)
        return self._send(msg)
        
    def add_vp(self, primary_key=None):
        msg = TSDBOp_AddVP(primary_key).to_json()
        print("C> add_vp msg", msg)
        return self._send(msg)   
        
    def simsearch(self, ts):
        msg = TSDBOp_SimSearch(ts).to_json()
        print("C> simsearch msg", msg)
        return self._send(msg)
        
    def sim_search_SAX(self, arg):
        #your code here
        msg = TSDBOp_SimsearchSAX(arg).to_json()
        return self._send(msg)
    
    def select(self, metadata_dict={}, fields=None, additional=None):
        #your code here
        msg = TSDBOp_Select(metadata_dict, fields,additional).to_json()
        return self._send(msg)
        
    def augmented_select(self, proc, target, arg=None, metadata_dict={}, additional=None):
        #your code here
        if arg == None and proc == 'corr':
            raise ValueError('Cannot use "corr" with no argument')
        msg = TSDBOp_AugmentedSelect(proc, target, arg, metadata_dict, additional).to_json()
        return self._send(msg)

    def add_trigger(self, proc, onwhat, target, arg):
        # your code here
        msg = TSDBOp_AddTrigger(proc, onwhat, target, arg).to_json()

        return self._send(msg)

    def remove_trigger(self, proc, onwhat):
        # your code here
        msg = TSDBOp_RemoveTrigger(proc, onwhat).to_json()
        return self._send(msg)



    def _send(self, msg):
        
        return serialize(msg)
    def decode(self,data):
        deserializer=Deserializer()
        deserializer.append(data)
        if deserializer.ready():
            msg=deserializer.deserialize()
            status = TSDBStatus(msg['status'])
            print ("C> status:", status)
            payload = msg['payload']
            print ("C> payload:", payload)
        return status, payload
 