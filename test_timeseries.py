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

suite = unittest.TestLoader().loadTestsFromModule(MyTest())
unittest.TextTestRunner().run(suite)
