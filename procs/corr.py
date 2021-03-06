import timeseries as ts
import numpy as np

from ._corr import stand, kernel_corr

import asyncio

# this function is directly used for augmented selects
def proc_main(pk, row, arg):
    #The argument is a time series. But due to serialization it does
    #not come out as the "instance", and must be cast
    argts = ts.TimeSeries(*arg)
    #compute a standardized time series
    stand_argts = stand(argts, argts.mean(), argts.std())
    # for each row in our select/etc, standardize the time series
    stand_rowts = stand(row['ts'], row['ts'].mean(), row['ts'].std())
    #compute the normalozed kernelized cross-correlation
    kerncorr = kernel_corr(stand_rowts, stand_argts, 5)
    # compute a distance from it.
    #The distance is given by np.sqrt(K(x,x) + K(y,y) - 2*K(x,y))
    #since we are normalized the autocorrs are 1
    kerndist = np.sqrt(2*(1-kerncorr)).real
    return [kerndist]

#the function is wrapped in a coroutine for triggers
async def main(pk, row, arg):
    return proc_main(pk, row, arg)
    
# Identical to proc_main except for the arguments, intended for
# use within the database class rather than as a trigger
def corr_indb(ts1, ts2):
   #compute a standardized time series
    stand_ts1 = stand(ts1, ts1.mean(), ts1.std())
    # for each row in our select/etc, standardize the time series
    stand_ts2 = stand(ts2, ts2.mean(), ts2.std())
    #compute the normalozed kernelized cross-correlation
    kerncorr = kernel_corr(stand_ts1, stand_ts2, 5)
    # compute a distance from it.
    #The distance is given by np.sqrt(K(x,x) + K(y,y) - 2*K(x,y))
    #since we are normalized the autocorrs are 1
    kerndist = np.sqrt(2*(1-kerncorr)).real
    return kerndist
