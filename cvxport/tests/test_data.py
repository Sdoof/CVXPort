
import unittest
import pandas as pd
from numpy.testing import assert_array_equal
import numpy as np
from cvxport.data import Bar, TimedBars, BarPanel, MT4DataObject
from cvxport.const import Freq


class TestBar(unittest.TestCase):
    def test_bar1(self):
        bar = Bar()
        bar.update(1)
        bar.update(0)
        bar.update(2.1)
        bar.update(2)
        self.assertEqual(1, bar.open)
        self.assertEqual(2.1, bar.high)
        self.assertEqual(0, bar.low)
        self.assertEqual(2, bar.close)

    def test_bar2(self):
        # single point
        bar = Bar()
        bar.update(1)
        self.assertEqual(1, bar.open)
        self.assertEqual(1, bar.high)
        self.assertEqual(1, bar.low)
        self.assertEqual(1, bar.close)

    def test_timed_bar(self):
        bars = TimedBars(['usd', 'eur'], Freq.MINUTE)
        res = []

        data = [
            # first bar
            [pd.Timestamp('2019-01-01 00:00:55'), 'usd', 1],
            [pd.Timestamp('2019-01-01 00:00:56'), 'usd', 1.1],
            [pd.Timestamp('2019-01-01 00:00:57'), 'eur', 2],
            [pd.Timestamp('2019-01-01 00:00:58'), 'eur', 1.9],
            [pd.Timestamp('2019-01-01 00:00:59'), 'usd', 0.9],
            [pd.Timestamp('2019-01-01 00:01:00'), 'usd', 1],

            # second bar
            [pd.Timestamp('2019-01-01 00:01:30'), 'eur', 2],  # this will be pick up in the 03:00 bar
            [pd.Timestamp('2019-01-01 00:03:00'), 'usd', 1],  # skip bar
        ]

        for t, k, v in data:
            res.append(bars(t, k, v))

        # first bar
        self.assertEqual([None] * 5, res[:5])
        self.assertEqual(pd.Timestamp('2019-01-01 00:01:00'), res[5][0])
        assert_array_equal([1.0, 2.0], res[5][1])
        assert_array_equal([1.1, 2], res[5][2])
        assert_array_equal([0.9, 1.9], res[5][3])
        assert_array_equal([1, 1.9], res[5][4])

        # second bar
        self.assertEqual(None, res[6])
        self.assertEqual(pd.Timestamp('2019-01-01 00:03:00'), res[7][0])
        assert_array_equal([1, 2], res[7][1])
        assert_array_equal([1, 2], res[7][2])
        assert_array_equal([1, 2], res[7][3])
        assert_array_equal([1, 2], res[7][4])

    def test_bar_panel(self):
        bars = BarPanel(['usd', 'eur'], Freq.MINUTE, 5)
        res = []

        data = [
            # first bar
            [pd.Timestamp('2019-01-01 00:00:55'), 'usd', 1],
            [pd.Timestamp('2019-01-01 00:00:56'), 'usd', 1.1],
            [pd.Timestamp('2019-01-01 00:00:57'), 'eur', 2],
            [pd.Timestamp('2019-01-01 00:00:58'), 'eur', 1.9],
            [pd.Timestamp('2019-01-01 00:00:59'), 'usd', 0.9],
            [pd.Timestamp('2019-01-01 00:01:00'), 'usd', 1],

            # second bar
            [pd.Timestamp('2019-01-01 00:01:30'), 'eur', 2],  # this will be pick up in the 03:00 bar
            [pd.Timestamp('2019-01-01 00:03:00'), 'usd', 1],  # skip bar

            # 3rd bar
            [pd.Timestamp('2019-01-01 00:04:00'), 'usd', 1],  # skip bar

            # 4th bar
            [pd.Timestamp('2019-01-01 00:04:01'), 'eur', 2],  # skip bar
            [pd.Timestamp('2019-01-01 00:04:10'), 'eur', 1.8],  # skip bar
            [pd.Timestamp('2019-01-01 00:04:30'), 'eur', 2.1],  # skip bar
            [pd.Timestamp('2019-01-01 00:04:40'), 'usd', 1.3],  # skip bar
            [pd.Timestamp('2019-01-01 00:05:00'), 'eur', 2.4],  # skip bar

            # 5th bar
            [pd.Timestamp('2019-01-01 00:06:00'), 'usd', 1],  # skip bar

            # 6th bar
            [pd.Timestamp('2019-01-01 00:07:00'), 'eur', 2],  # skip bar
        ]

        for t, k, v in data:
            res.append(bars(t, k, v))

        # 1st bar
        # we don't test time idx here because we use list to store them and it becomes the exactly the same for
        # each period. This shouldn't be an issue in practice
        self.assertEqual([None] * 5, res[:5])
        assert_array_equal([[1.0, 2.0]], res[5][1]['open'])
        assert_array_equal([[1.1, 2]], res[5][1]['high'])
        assert_array_equal([[0.9, 1.9]], res[5][1]['low'])
        assert_array_equal([[1, 1.9]], res[5][1]['close'])

        # 2nd bar
        self.assertEqual(None, res[6])
        assert_array_equal([[1.0, 2.0], [1, 2]], res[7][1]['open'])
        assert_array_equal([[1.1, 2], [1, 2]], res[7][1]['high'])
        assert_array_equal([[0.9, 1.9], [1, 2]], res[7][1]['low'])
        assert_array_equal([[1, 1.9], [1, 2]], res[7][1]['close'])

        # 3rd bar
        assert_array_equal([[1.0, 2.0], [1, 2], [1, np.nan]], res[8][1]['open'])
        assert_array_equal([[1.1, 2], [1, 2], [1, np.nan]], res[8][1]['high'])
        assert_array_equal([[0.9, 1.9], [1, 2], [1, np.nan]], res[8][1]['low'])
        assert_array_equal([[1, 1.9], [1, 2], [1, np.nan]], res[8][1]['close'])

        # 4rd bar
        self.assertEqual([None] * 4, res[9: 13])
        assert_array_equal([[1.0, 2.0], [1, 2], [1, np.nan], [1.3, 2]], res[13][1]['open'])
        assert_array_equal([[1.1, 2], [1, 2], [1, np.nan], [1.3, 2.4]], res[13][1]['high'])
        assert_array_equal([[0.9, 1.9], [1, 2], [1, np.nan], [1.3, 1.8]], res[13][1]['low'])
        assert_array_equal([[1, 1.9], [1, 2], [1, np.nan], [1.3, 2.4]], res[13][1]['close'])

        # 5th bar
        assert_array_equal([[1.0, 2.0], [1, 2], [1, np.nan], [1.3, 2], [1, np.nan]], res[14][1]['open'])
        assert_array_equal([[1.1, 2], [1, 2], [1, np.nan], [1.3, 2.4], [1, np.nan]], res[14][1]['high'])
        assert_array_equal([[0.9, 1.9], [1, 2], [1, np.nan], [1.3, 1.8], [1, np.nan]], res[14][1]['low'])
        assert_array_equal([[1, 1.9], [1, 2], [1, np.nan], [1.3, 2.4], [1, np.nan]], res[14][1]['close'])

        # 6th bar
        # we test time idx here since it's the final one
        self.assertEqual([
            pd.Timestamp('2019-01-01 00:03:00'),
            pd.Timestamp('2019-01-01 00:04:00'),
            pd.Timestamp('2019-01-01 00:05:00'),
            pd.Timestamp('2019-01-01 00:06:00'),
            pd.Timestamp('2019-01-01 00:07:00')
        ], res[15][0])
        assert_array_equal([[1, 2], [1, np.nan], [1.3, 2], [1, np.nan], [np.nan, 2]], res[15][1]['open'])
        assert_array_equal([[1, 2], [1, np.nan], [1.3, 2.4], [1, np.nan], [np.nan, 2]], res[15][1]['high'])
        assert_array_equal([[1, 2], [1, np.nan], [1.3, 1.8], [1, np.nan], [np.nan, 2]], res[15][1]['low'])
        assert_array_equal([[1, 2], [1, np.nan], [1.3, 2.4], [1, np.nan], [np.nan, 2]], res[15][1]['close'])


class TestMT4DataObject(unittest.TestCase):
    def test_mt4(self):
        data_obj = MT4DataObject(['usd', 'eur'], 12345)

        async def get_data():
            res = []
            for _ in range(3):
                pass


if __name__ == '__main__':
    unittest.main()