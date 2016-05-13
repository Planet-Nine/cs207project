import timeseries as ts
import numpy as np
import scipy
from scipy.stats import norm
from ._corr import stand, kernel_corr

#from ._corr import stand, kernel_corr

import asyncio

Breakpoints = {}
Breakpoints[2] = np.array([0.]) 
Breakpoints[4] = np.array([-0.67449,0,0.67449]) 
Breakpoints[8] = np.array([-1.1503,-0.67449,-0.31864,0,0.31864,0.67449,1.1503]) 
Breakpoints[16] = np.array([-1.5341,-1.1503,-0.88715,-0.67449,-0.48878,-0.31864,-0.15731,0,0.15731,0.31864,0.48878,0.67449,0.88715,1.1503,1.5341]) 
Breakpoints[32] = np.array([-1.8627,-1.5341,-1.318,-1.1503,-1.01,-0.88715,-0.77642,-0.67449,-0.57913,-0.48878,-0.40225,-0.31864,-0.2372,-0.15731,-0.078412,0,0.078412,0.15731,0.2372,0.31864,0.40225,0.48878,0.57913,0.67449,0.77642,0.88715,1.01,1.1503,1.318,1.5341,1.8627]) 
Breakpoints[64] = np.array([-2.1539,-1.8627,-1.6759,-1.5341,-1.4178,-1.318,-1.2299,-1.1503,-1.0775,-1.01,-0.94678,-0.88715,-0.83051,-0.77642,-0.72451,-0.67449,-0.6261,-0.57913,-0.53341,-0.48878,-0.4451,-0.40225,-0.36013,-0.31864,-0.27769,-0.2372,-0.1971,-0.15731,-0.11777,-0.078412,-0.039176,0,0.039176,0.078412,0.11777,0.15731,0.1971,0.2372,0.27769,0.31864,0.36013,0.40225,0.4451,0.48878,0.53341,0.57913,0.6261,0.67449,0.72451,0.77642,0.83051,0.88715,0.94678,1.01,1.0775,1.1503,1.2299,1.318,1.4178,1.5341,1.6759,1.8627,2.1539]) 
# this function is directly used for augmented selects
def proc_main(pk, row, arg):
    #your code here
    argts = ts.TimeSeries(*arg)
    series = stand(argts,argts.mean(),argts.std())
    a = 4
    w = 32
    symbols = ['{0:b}'.format(i).zfill(int(np.log(a-1)/np.log(2))+1) for i in range(a)]
    if a in Breakpoints:
        breakpoints = Breakpoints[a]#norm.ppf(np.array([i/a for i in range(1,a)]))
    else:
        raise ValueError('Breakpoints do not exist for cardinality {}'.format(a)) 
    breakpoints = np.array([*breakpoints,np.inf])
    T = np.zeros((w))
    n = len(series)
    SAX = []
    for i in range(w):
        T[i] = w/n*sum([series.data[j] for j in range(int(n/w*i),int(n/w*(i+1)))])
        for j in range(a):
            if j == a-1:
                SAX.append(symbols[j])
                break
            if T[i]<breakpoints[0]:
                SAX.append(symbols[0])
                break
            if T[i]>breakpoints[j] and T[i]<breakpoints[j+1]:
                SAX.append(symbols[j+1])
                break
    return SAX
    
#the function is wrapped in a coroutine for triggers
async def main(pk, row, arg):
    return proc_main(pk, row, arg)
    
# Identical to proc_main except for the arguments, intended for
# use within the database class rather than as a trigger
def isax_indb(ts1,a,w,switch=0):
    series = stand(ts1,ts1.mean(),ts1.std())
    symbols = ['{0:b}'.format(i).zfill(int(np.log(a-1)/np.log(2))+1) for i in range(a)]
    if a in Breakpoints:
        breakpoints = Breakpoints[a]
    elif '1' not in '{0:b}'.format(wordlength)[1:]:
        breakpoints = norm.ppf(np.array([i/a for i in range(1,a)]))
    else:
        raise ValueError('Breakpoints do not exist for cardinality {}'.format(a))
    breakpoints = np.array([*breakpoints,np.inf])
    T = np.zeros((w))
    if switch == 0:
        n = len(series)
    else:
        n = series.time[-1]-series.time[0]
    SAX = []
    for i in range(w):
        if switch == 0:
            T[i] = w/n*sum([series.data[j] for j in range(int(n/w*i),int(n/w*(i+1)))])
        else:
            interval = series.data[np.intersect1d(np.where(n/w*(i+1) >= series.time-series.time[0] ),np.where(n/w*i <= series.time-series.time[0]))]
            T[i] = w/n*sum(interval)
        for j in range(a):
            if j == a-1:
                SAX.append(symbols[j])
                break
            if T[i]<=breakpoints[0]:
                SAX.append(symbols[0])
                break
            if T[i]>breakpoints[j] and T[i]<=breakpoints[j+1]:
                SAX.append(symbols[j+1])
                break
    return SAX
