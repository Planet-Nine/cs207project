import unittest
from timeseries import TimeSeries

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


suite = unittest.TestLoader().loadTestsFromModule(MyTest())
unittest.TextTestRunner().run(suite)
