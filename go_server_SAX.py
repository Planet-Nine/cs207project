#!/usr/bin/env python3
from tsdb import TSDBServer
from tsdb.persistentdb import PersistentDB
import timeseries as ts
import tkinter as tk



identity = lambda x: x

schema = {
  'pk': {'convert': identity, 'index': None,'type':str},  #will be indexed anyways
  'ts': {'convert': identity, 'index': None},
  'order': {'convert': int, 'index': 1,'type':int},
  'blarg': {'convert': int, 'index': 1,'type' : int},
  'useless': {'convert': identity, 'index': None, 'type' : str},
  'mean': {'convert': float, 'index': 1,'type' : float},
  'std': {'convert': float, 'index': 1, 'type' : float},
  'vp': {'convert': bool, 'index': 1, 'type' : bool}
}

#schema = {
#  'pk': {'convert': identity, 'index': None, 'type':str},  #will be indexed anyways
#  'ts': {'convert': identity, 'index': None},
#}

NUMVPS = 5


def main(load=False, dbname="db", overwrite=False, threshold = 10, wordlength = 16, tslen = 256, cardinality = 64):
    # we augment the schema by adding columns for 5 vantage points
    #for i in range(NUMVPS):
    #    schema["d_vp-{}".format(i)] = {'convert': float, 'index': 1}
    db = PersistentDB(schema, 'pk',load=load, dbname=dbname, overwrite=overwrite, threshold = threshold, wordlength = wordlength, tslen = tslen, cardinality = cardinality)
    server = TSDBServer(db)
    server.run()

if __name__=='__main__':
    class SampleApp(tk.Tk):
        def __init__(self):
            tk.Tk.__init__(self)
            self.destroy()
            self.master = tk.Tk()
            self.master2 = None
            self.label = tk.Label(self.master,text="""
            This is a brief introduction to the similarity search for time series              
            using the iSAX index. 

            In the next window you will be asked to input some values. The first 
            determines whether to load or not the database from some existing database.          
            You will next be asked to provide a database name for loading or writing. 
            Next, whether to overwrite the existing database, in the event that one of 
            the given name exists. You will further be asked to give the cardinality of 
            the iSAX representation to be used, which is essentially the number of vertical 
            slices into which you wish the time series to be divided for indexing. 
            Cardinalities greater than 64 are not supported at the moment. The next 
            value is a threshold, the number of time series to hold in a leaf node. Then the 
            world length, so the number of segments or horizontal slices for indexing the 
            time series. Finally, please provide the time series length, which is the number 
            of data points you wish your time series to be interpolated to for uniformization 
            of the time series. These interpolation points will be evenly spaced between the 
            maximum and minimum time values, including the endpoints. If no values are input        
            defaults will be used. Defaults are indicated by [...].
            """,justify = 'left') 
            self.button = tk.Button(self.master, text="continue", command=self.on_button)
            self.label.pack()
            self.button.pack()

        def on_button(self):
            if self.master2:
                self.master2.destroy()
            else:
                self.master.destroy()
            self.card = 64
            self.dbn = "db"
            self.th = 10
            self.wl = 8
            self.tslen = 256
            self.master1 = tk.Tk()
            self.label2 = tk.Label(self.master1,text="Load (true or false) [False]: ")
            self.entry2 = tk.Entry(self.master1)
            self.label3 = tk.Label(self.master1,text="Database name (no spaces) [db]: ")
            self.entry3 = tk.Entry(self.master1)
            self.label4 = tk.Label(self.master1,text="Overwrite (true or false) [False]: ")
            self.entry4 = tk.Entry(self.master1)
            self.label1 = tk.Label(self.master1,text="Cardinality (must be a power of 2) [64]: ") 
            self.entry1 = tk.Entry(self.master1)
            self.label5 = tk.Label(self.master1,text="Threshold (must be a positive integer) [10]: ")
            self.entry5 = tk.Entry(self.master1)
            self.label6 = tk.Label(self.master1,text="Word length (must be a power of 2) [8]: ")
            self.entry6 = tk.Entry(self.master1)
            self.label7 = tk.Label(self.master1,text="Time series length (must be a power of 2) [256]: ")
            self.entry7 = tk.Entry(self.master1)

            self.button = tk.Button(self.master1, text="continue", command=self.on_button1)
            self.label2.pack()
            self.entry2.pack()
            self.label3.pack()
            self.entry3.pack()
            self.label4.pack()
            self.entry4.pack()
            self.label1.pack()
            self.entry1.pack()
            self.label5.pack()
            self.entry5.pack()
            self.label6.pack()
            self.entry6.pack()
            self.label7.pack()
            self.entry7.pack()
            self.button.pack()

        def on_button1(self):
            self.master2 = tk.Tk()
            card = self.entry1.get()
            if card:
                try:
                    self.card = int(card)
                except:
                    self.label_1 = tk.Label(self.master2,text="Please enter a number for the cardinality.")
                    self.button1 = tk.Button(self.master2, text="continue", command=self.on_button)
                    self.master1.destroy()
                    self.label_1.pack()
                    self.button1.pack()
            self.ld = self.entry2.get()
            if self.ld:
                if self.ld[0].lower() == 't':
                    self.ld = True
                else:
                    self.ld = False
            else:
                self.ld = False
            dbn = self.entry3.get()
            if dbn:
                self.dbn = dbn
            self.ovrw = self.entry4.get()
            if self.ovrw:
                if self.ovrw[0].lower() == 't':
                    self.ovrw = True
                else:
                    self.ovrw = False
            else:
                self.ovrw = False
            th = self.entry5.get()
            wl = self.entry6.get()
            tslen = self.entry7.get()
            if th:
                try:
                    self.th = int(th)
                except:
                    self.label_1 = tk.Label(self.master2,text="Please enter a number for the threshold.")
                    self.button1 = tk.Button(self.master2, text="continue", command=self.on_button)
                    self.master1.destroy()
                    self.label_1.pack()
                    self.button1.pack()
            if wl:
                try:
                    self.wl = int(wl)
                except:
                    self.label_1 = tk.Label(self.master2,text="Please enter a number for the word length.")
                    self.button1 = tk.Button(self.master2, text="continue", command=self.on_button)
                    self.master1.destroy()
                    self.label_1.pack()
                    self.button1.pack()
            if tslen:
                try:
                    self.tslen = int(tslen)
                except:
                    self.label_1 = tk.Label(self.master2,text="Please enter a number for the time series length.")
                    self.button1 = tk.Button(self.master2, text="continue", command=self.on_button)
                    self.master1.destroy()
                    self.label_1.pack()
                    self.button1.pack()
            
            self.label_1 = tk.Label(self.master2,text="Is the following correct?\n\nLoad: "+str(self.ld)+'\n\nDatabase name: '+str(self.dbn)+'\n\nOverwrite: '+str(self.ovrw)+'\n\nCardinality: '+str(self.card)+'\n\nThreshold: '+str(self.th)+'\n\nWord length: '+str(self.wl)+'\n\nTime series length: '+str(self.tslen)+'\n\n',justify = 'left')
            self.button1 = tk.Button(self.master2, text="yes", command=self.on_button2)
            self.button2 = tk.Button(self.master2, text="no", command=self.on_button)
            self.master1.destroy()

            self.label_1.pack()
            self.button1.pack(side='right')
            self.button2.pack(side='right')

        def on_button2(self):
            self.master2.destroy()
            main(load=self.ld, dbname=self.dbn, overwrite=self.ovrw, threshold = self.th, wordlength = self.wl, tslen = self.tslen, cardinality = self.card)
        

    app = SampleApp()
    app.mainloop()
    