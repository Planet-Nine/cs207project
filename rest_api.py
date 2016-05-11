import asyncio
import json
import timeseries as ts
from tsdb.tsdb_serialization import serialize, LENGTH_FIELD_LENGTH, Deserializer
import tornado.concurrent
import tornado.ioloop
import tornado.web
import tornado.platform.asyncio
import tornado.httpclient
from tsdb import rest
from tsdb.tsdb_ops import *
from tsdb.tsdb_error import *
import ast
class InsertHandler(tornado.web.RequestHandler):
    def initialize(self,loop):
        self.loop=loop
    async def post(self):
        client = rest.TSDBrestapi()
        # testts = ts.TimeSeries([1,2,3],[4,5,6])
        pk = self.get_argument('pk')
        print('pk: ', pk)
        data = self.get_argument('ts')
        print('data: ',data)
        timeseries = ts.TimeSeries(*ast.literal_eval(data))
        # ts = ts.TimeSeries(*ts)
        print('timeseries: ',timeseries)
        r = client.insert_ts(pk,timeseries)
        status, payload = await tcp_echo_client(r,self.loop)
        status = TSDBStatus(status)
        print(status)
        self.write({'status':str(status),'payload':payload})
class VpHandler(tornado.web.RequestHandler):
    def initialize(self,loop):
        self.loop=loop
    async def post(self):
        client = rest.TSDBrestapi()
        pk = self.get_argument('pk',None)
        r = client.add_vp(pk)
        status, payload = await tcp_echo_client(r,self.loop)
        status = TSDBStatus(status)
        print(status)
        self.write({'status':str(status),'payload':payload})
class DeleteHandler(tornado.web.RequestHandler):
    def initialize(self,loop):
        self.loop=loop
    async def post(self):
        client = rest.TSDBrestapi()
        # testts = ts.TimeSeries([1,2,3],[4,5,6])
        pk = self.get_argument('pk')
        r = client.delete_ts(pk)
        status, payload = await tcp_echo_client(r,self.loop)
        status = TSDBStatus(status)
        print(status)
        self.write({'status':str(status),'payload':payload})
        
class UpsertHandler(tornado.web.RequestHandler):
    def initialize(self,loop):
        self.loop=loop
    async def post(self):
        client = rest.TSDBrestapi()
        # testts = ts.TimeSeries([1,2,3],[4,5,6])
        pk = self.get_argument('pk')
        print('pk: ', pk)
        data = self.get_argument('meta_dict')
        print('data: ',data)
        metadata_dict = ast.literal_eval(data)
        # ts = ts.TimeSeries(*ts)
        r = client.upsert_meta(pk,metadata_dict)
        status, payload = await tcp_echo_client(r,self.loop)
        status = TSDBStatus(status)
        print(status)
        self.write({'status':str(status),'payload':payload})
        
class SelectHandler(tornado.web.RequestHandler):
    def initialize(self,loop):
        self.loop=loop
    async def get(self):
        client = rest.TSDBrestapi()
        # testts = ts.TimeSeries([1,2,3],[4,5,6])
        metadata_dict = ast.literal_eval(self.get_argument('meta_dict','{}'))
        fields = ast.literal_eval(self.get_argument('fields','None'))
        additional = ast.literal_eval(self.get_argument('additional', 'None'))
        # ts = ts.TimeSeries(*ts)
        r = client.select(metadata_dict,fields,additional)
        status, payload = await tcp_echo_client(r,self.loop)
        status = TSDBStatus(status)
        print(status)
        self.write({'status':str(status),'payload':payload})
        
class AugSelectHandler(tornado.web.RequestHandler):
    def initialize(self,loop):
        self.loop=loop
    async def get(self):
        client = rest.TSDBrestapi()
        # testts = ts.TimeSeries([1,2,3],[4,5,6])
        proc = self.get_argument('proc')
        target = ast.literal_eval(self.get_argument('target'))
        arg = ts.TimeSeries(*ast.literal_eval(self.get_argument('arg')))
        metadata_dict = ast.literal_eval(self.get_argument('meta_dict','{}'))
        fields = ast.literal_eval(self.get_argument('fields','None'))
        additional = ast.literal_eval(self.get_argument('additional', 'None'))
        # ts = ts.TimeSeries(*ts)
        r = client.augmented_select(proc, target, arg, metadata_dict, additional)
        status, payload = await tcp_echo_client(r,self.loop)
        status = TSDBStatus(status)
        print(status)
        self.write({'status':str(status),'payload':payload})
        
class SimSearchHandler(tornado.web.RequestHandler):
    def initialize(self,loop):
        self.loop=loop
    async def get(self):
        client = rest.TSDBrestapi()
        timeseries = ts.TimeSeries(*ast.literal_eval(self.get_argument('ts')))        
        r = client.simsearch(timeseries)
        status, payload = await tcp_echo_client(r,self.loop)
        status = TSDBStatus(status)
        print(status)
        self.write({'status':str(status),'payload':payload})
        
async def tcp_echo_client(message,loop,port=9999,host='127.0.0.1'):
    reader, writer = await asyncio.open_connection(host, port,
                                                        loop=loop)

    print('Send: %r' % message)
    writer.write(message)
    status= None
    payload=None
    data = await reader.read(8192)
    print('C>received message')
    deserializer=Deserializer()
    deserializer.append(data)
    if deserializer.ready():
        msg=deserializer.deserialize()
        status = msg['status']
        print ("C> status:", status)
        payload = msg['payload']
        print ("C> payload:", payload)
    return status, payload

    print('Close the socket')
    writer.close()

if __name__ == '__main__':
  tornado.platform.asyncio.AsyncIOMainLoop().install()
  
  loop = asyncio.get_event_loop()
  app = tornado.web.Application([
  (r'/insert_ts', InsertHandler,dict(loop=loop)),
  (r'/add_vp', InsertHandler,dict(loop=loop)),
  (r'/delete', DeleteHandler,dict(loop=loop)),
  (r'/upsert_meta', UpsertHandler,dict(loop=loop)),
  (r'/select', SelectHandler,dict(loop=loop)),
  (r'/augmented_select', AugSelectHandler,dict(loop=loop)),
  (r'/simsearch', SimSearchHandler,dict(loop=loop)),
  ])
  app.listen(8080)
  loop.run_forever()