import asyncio
from .tsdb_serialization import serialize, LENGTH_FIELD_LENGTH, Deserializer
from .tsdb_ops import *
from .tsdb_error import *

class TSDBClient(object):
    "client"
    def __init__(self, port=9999):
        self.port = port

    def insert_ts(self, primary_key, ts):
        #your code here, construct from the code in tsdb_ops.py
        msg = TSDBOp_InsertTS(primary_key, ts).to_json()
        print("C> insert_ts msg", msg)
        self._send(msg)

    def upsert_meta(self, primary_key, metadata_dict):
        msg = TSDBOp_UpsertMeta(primary_key, metadata_dict).to_json()
        print("C> upsert msg", msg)
        self._send(msg)

    def select(self, metadata_dict={}, fields=None, additional=None):
        #your code here
        msg = TSDBOp_Select(metadata_dict, fields,additional).to_json()
        print("C> select msg", msg)
        self._send(msg)
        
    def augmented_select(self, proc, target, arg=None, metadata_dict={}, additional=None):
        #your code here
        msg = TSDBOp_AugmentedSelect(proc, target, arg, metadata_dict, additional).to_json()
        print("C> aug select msg", msg)
        self._send(msg)

    def add_trigger(self, proc, onwhat, target, arg):
        # your code here
        msg = TSDBOp_AddTrigger(proc, onwhat, target, arg).to_json()
        print("C> addtrigger msg", msg)
        self._send(msg)

    def remove_trigger(self, proc, onwhat):
        # your code here
        msg = TSDBOp_RemoveTrigger(proc, onwhat).to_json()
        print("C> removetrigger msg", msg)
        self._send(msg)

    async def _send_coro(self, msg, loop):
        #your code here
        prot = await loop.create_connection(lambda: TSDBClientProtocol(msg, loop),
                              '127.0.0.1', self.port)
        status = prot[1].status
        payload = prot[1].payload
        return status, payload

    def _send(self, msg):
        loop = asyncio.get_event_loop()
        coro = asyncio.ensure_future(self._send_coro(msg, loop))
        loop.run_until_complete(coro)
        return coro.result()
        
class TSDBClientProtocol(asyncio.Protocol):
    def __init__(self,message,loop):
        self.deserializer=Deserializer()
        self.msg=message
        self.loop=loop
        self.status=None
        self.payload=None
    def connection_made(self,conn):
        self.conn=conn
        print("C>connection made, writing")
        self.conn.write(serialize(self.msg))
        # self.conn.close()
    def data_received(self,response):
        # print('C>received data!')
        self.deserializer.append(response)
        if self.deserializer.ready():
            msg=self.deserializer.deserialize()
            print('C>received message:',msg)
            self.status = TSDBStatus(msg['status'])
            print ("C> status:", self.status)
            self.payload = msg['payload']
            print ("C> payload:", self.payload)
        # self.conn.close()
    # def connection_lost(self,transport):
        # print("C> connection lost")
        # self.loop.stop()
    
    