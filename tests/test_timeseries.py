import unittest
from timeseries.timeseries import TimeSeries
import numpy as np

class MyTest(unittest.TestCase):

    def test_median(self):
        self.assertEqual(TimeSeries([1,2,3],[2,2,2]).median(),2)
        self.assertEqual(TimeSeries([1,2,3],[0,2,0]).median(),0)
        self.assertEqual(TimeSeries([1,2,3,4],[0,2,2,0]).median(),1)
        with self.assertRaises(ValueError):
            TimeSeries([],[]).median()

    def test_mean(self):
        self.assertEqual(TimeSeries([1,2,3],[2,2,2]).mean(),2)
        self.assertEqual(TimeSeries([1,2,3],[0,2,0]).mean(),2/3)
        self.assertEqual(TimeSeries([1,2,3,4],[0,2,2,0]).mean(),1)
        with self.assertRaises(ValueError):
            TimeSeries([],[]).mean()

    def test_iters(self):
        ts = TimeSeries([1,2,3],[4,5,6])
        times = ts.times()

        count = 0
        for item in ts:
            self.assertEqual(item, ts[times[count]])
            count += 1
        self.assertEqual(count, len(ts))

        count = 0
        for item in ts.itervalues():
            self.assertEqual(item, ts[times[count]])
            count += 1
        self.assertEqual(count, len(ts))

        count = 0
        for item in ts.itertimes():
            self.assertEqual(item, times[count])
            count += 1
        self.assertEqual(count, len(ts))

        count = 0
        for item in ts.iteritems():
            self.assertEqual(item, (times[count], ts[times[count]]))
            count += 1
        self.assertEqual(count, len(ts))

    def test_pos(self):
        self.assertListEqual( list(TimeSeries([1,2,3],[-1,2,-4]).__pos__()) , [-1,2,-4] )
        self.assertListEqual( list(TimeSeries([1,2,3],[1,2,4]).__pos__() ), [1,2,4]  )
        self.assertListEqual( list(TimeSeries([1,2,3],[-4,-6,-7]).__pos__() ), [-4,-6,-7] )
        with self.assertRaises(ValueError):
            TimeSeries([],[]).__pos__()    

    def test_neg(self):
        self.assertListEqual( list( TimeSeries([1,2,3],[-1,2,-4]).__neg__() ), [1,-2,4]  )
        self.assertListEqual( list( TimeSeries([1,2,3],[1,2,4]).__neg__() ), [-1,-2,-4]  )
        self.assertListEqual( list( TimeSeries([1,2,3],[-4,-6,-7]).__neg__() ), [4,6,7] )
        with self.assertRaises(ValueError):
            TimeSeries([],[]).__neg__() 

    def test_abs(self):
        self.assertEqual( abs(TimeSeries([1,2,3],[-1,2,-4])), np.sqrt(21)  )
        self.assertEqual( abs(TimeSeries([1,2,3],[1,2,4])), np.sqrt(21)  )
        self.assertEqual( abs(TimeSeries([1,2,3],[0,0,0])), 0 )
        with self.assertRaises(ValueError):
            abs(TimeSeries([],[]))

    def test_bool(self):
        self.assertEqual( bool(TimeSeries([1,2,3],[-1,2,-4])), True )
        self.assertEqual( bool(TimeSeries([1,2,3],[1,2,4])), True)
        self.assertEqual( bool(TimeSeries([1,2,3],[0,0,0])), False)

    def test_add(self):
        a = TimeSeries([1,2,3], [1,2,3])
        b = TimeSeries([1,2,3], [4,5,6])
        c = TimeSeries([4,5,6], [4,5,6])
        self.assertEqual(a+b, TimeSeries([1,2,3],[5,7,9]))
        with self.assertRaises(ValueError):
            d = b + c

    def test_mul(self):
        a = TimeSeries([1,2,3], [1,2,3])
        b = TimeSeries([1,2,3], [4,5,6])
        c = TimeSeries([4,5,6], [4,5,6])
        self.assertEqual(a*b, TimeSeries([1,2,3],[4,10,18]))
        with self.assertRaises(ValueError):
            d = b * c

suite = unittest.TestLoader().loadTestsFromModule(MyTest())
unittest.TextTestRunner().run(suite)
