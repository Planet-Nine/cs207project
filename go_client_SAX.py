#!/usr/bin/env python3
from tsdb import TSDBClient
import timeseries as ts
import numpy as np
import tkinter as tk
   
from scipy.stats import norm

# m is the mean, s is the standard deviation, and j is the jitter
# the meta just fills in values for order and blarg from the schema
def tsmaker(m, s, j):
    "returns metadata and a time series in the shape of a jittered normal"
    meta={}
    meta['order'] = int(np.random.choice([-5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5]))
    meta['blarg'] = int(np.random.choice([1, 2]))
    t = np.arange(0.0, 1.0, 0.01)
    v = norm.pdf(t, m, s) + j*np.random.randn(100)
    return meta, ts.TimeSeries(t, v)



def tsgenerator(client,number):
    mus = np.random.uniform(low=0.0, high=1.0, size=number)
    sigs = np.random.uniform(low=0.05, high=0.4, size=number)
    jits = np.random.uniform(low=0.05, high=0.2, size=number)

    # dictionaries for time series and their metadata
    tsdict={}
    metadict={}
    for i, m, s, j in zip(range(number), mus, sigs, jits):
        meta, tsrs = tsmaker(m, s, j)
        # the primary key format is ts-1, ts-2, etc
        pk = "ts-{}".format(i)
        tsdict[pk] = tsrs
        meta['vp'] = False # augment metadata with a boolean asking if this is a  VP.
        metadict[pk] = meta

    # Having set up the triggers, now inser the time series, and upsert the metadata
    for k in tsdict:
        client.insert_ts(k, tsdict[k])
        client.upsert_meta(k, metadict[k])
    return tsdict

if __name__=='__main__':
    class SampleApp2(tk.Tk):
        def __init__(self):
            tk.Tk.__init__(self)
            self.destroy()
            self.switch = 0
            self.switch2 = 0
            self.master3 = None
            self.master = tk.Tk()
            self.master2 = None
            self.label = tk.Label(self.master,text="""
            I would like to take this opportunity to thank you for using similarity search    
            for time series using the iSAX index. Please complete the prompts from the 
            other window before moving on to this window.
            """,justify = 'left') 
            self.button = tk.Button(self.master, text="continue", command=self.on_button)
            self.label.pack()
            self.button.pack()

        def on_button(self):
            self.client = TSDBClient()
            self.client.add_trigger('junk', 'insert_ts', None, 'db:one:ts')
            self.client.add_trigger('stats', 'insert_ts', ['mean', 'std'], None)
            if self.master2:
                self.master2.destroy()
            else:
                self.master.destroy()
            _, results = self.client.select()
            if not len(results):
                self.master1 = tk.Tk()
                self.label = tk.Label(self.master1,text="""
            It appears you have not chosen to load any time series into the database so the    
            database will be automatically populated for the search.
            
            """,justify= 'left')
                self.label2 = tk.Label(self.master1,text= "How many time series would you like in the database?") 
                self.entry = tk.Entry(self.master1)
                self.button = tk.Button(self.master1, text="continue", command=self.on_button1)
                self.label.pack()
                self.label2.pack()
                self.entry.pack()
                self.button.pack()
            else:
                self.switch = 1
                self.on_button1()
            
        def on_button1(self):
            self.master2 = tk.Tk()
            if self.switch == 1:
                self.label_1 = tk.Label(self.master2, text='Do you wish to provide your own time series for the query?')
                self.button1 = tk.Button(self.master2, text="yes", command=self.on_button2)
                self.button2 = tk.Button(self.master2, text="no", command=self.on_button3)
                self.label_1.pack()
                self.button1.pack(side='right')
                self.button2.pack(side='right')
            else:    
                self.number = None
                number = self.entry.get()
                if number:
                    try:
                        self.number = int(number)
                    except:
                        self.number = None
                if not self.number:
                    self.label_1 = tk.Label(self.master2,text="Please enter a number.")
                    self.button1 = tk.Button(self.master2, text="continue", command=self.on_button)
                    self.master1.destroy()
                    self.label_1.pack()
                    self.button1.pack()
                else:
                    self.master1.destroy()
                    self.tsdict = tsgenerator(self.client,self.number)
                    self.label_1 = tk.Label(self.master2, text='Do you wish to provide your own time series for the query?')
                    self.button1 = tk.Button(self.master2, text="yes", command=self.on_button2)
                    self.button2 = tk.Button(self.master2, text="no", command=self.on_button3)
                    self.label_1.pack()
                    self.button1.pack(side='right')
                    self.button2.pack(side='right')
                                        
        def on_button2(self):
            self.switch2 = 0
            if self.master3:
                self.master3 = tk.Tk()
                self.master4.destroy()
            else:
                self.master2.destroy()
                self.master3 = tk.Tk()
            self.label_1 = tk.Label(self.master3,text="Please enter the file name for the query (as .npy file): ")
            self.entry = tk.Entry(self.master3)
            self.button1 = tk.Button(self.master3, text="continue", command=self.on_button4)
            self.label_1.pack()
            self.entry.pack()
            self.button1.pack()
        
        def on_button3(self):
            self.master2.destroy()
            self.master4 = tk.Tk()
            self.switch2 = 1
            self.label = tk.Label(self.master4,text="A time series will be automatically generated. ")
            self.button1 = tk.Button(self.master4, text="continue", command=self.on_button4)
            self.label.pack()
            self.button1.pack()
        
        def on_button4(self):
            if self.switch2 == 0:
                self.filename = self.entry.get()
                if self.filename:
                    try:
                        tsarray = np.load(self.filename)
                        self.arg = ts.TimeSeries(tsarray[0,:], tsarray[1,:]) 
                        self.master3.destroy()
                    except:
                        self.master4 = tk.Tk()
                        self.label = tk.Label(self.master4,text="Error encountered, please make sure the file name is correct.")
                        self.button1 = tk.Button(self.master4, text="continue", command=self.on_button2)
                        self.master3.destroy()
                        self.label.pack()
                        self.button1.pack()
                else:
                    self.on_button2()
            else:
                mus = np.random.uniform(low=0.0, high=1.0, size=1)
                sigs = np.random.uniform(low=0.05, high=0.4, size=1)
                jits = np.random.uniform(low=0.05, high=0.2, size=1)
                self.master4.destroy()
                meta, tsrs = tsmaker(mus, sigs, jits)
 
                self.arg = tsrs
            
            import matplotlib.pyplot as plt
 
            _, res = self.client.sim_search_SAX(self.arg)
            if res:
                _, res2 = self.client.select(metadata_dict={'pk':res}, fields=['ts'])
                plt.plot(self.arg.times(), self.arg.values(),label='query')
                plt.plot(res2[res]['ts'][0], res2[res]['ts'][1],label='closest match')
                plt.legend(loc='best',fontsize=12)
                plt.show()
            else:
                print('Could not find matching time series')

            

    app = SampleApp2()
    app.mainloop()
