
import unittest
import pandas as pd
from numpy.testing import assert_array_equal, assert_array_almost_equal
import numpy as np
import zmq.asyncio as azmq
import zmq
import asyncio
import psycopg2 as pg
from datetime import datetime, timedelta
from pytz import timezone

from cvxport import Config
from cvxport.data import DataStore, Datum, Asset
from cvxport.data import TickAggregator, TimedBars, BarPanel, MT4DataObject, DownSampledBar
from cvxport.const import Freq, Broker
from cvxport import const


class TestBar(unittest.TestCase):
    def test_bar1(self):
        bar = TickAggregator()
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
        bar = TickAggregator()
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
        self.assertEqual([None] * 5, res[:5])
        self.assertEqual(pd.Timestamp('2019-01-01 00:01:00'), res[5][0])
        assert_array_equal([[1.0, 2.0]], res[5][1]['open'])
        assert_array_equal([[1.1, 2]], res[5][1]['high'])
        assert_array_equal([[0.9, 1.9]], res[5][1]['low'])
        assert_array_equal([[1, 1.9]], res[5][1]['close'])

        # 2nd bar
        self.assertEqual(None, res[6])
        self.assertEqual(pd.Timestamp('2019-01-01 00:03:00'), res[7][0])
        assert_array_equal([[1.0, 2.0], [1, 2]], res[7][1]['open'])
        assert_array_equal([[1.1, 2], [1, 2]], res[7][1]['high'])
        assert_array_equal([[0.9, 1.9], [1, 2]], res[7][1]['low'])
        assert_array_equal([[1, 1.9], [1, 2]], res[7][1]['close'])

        # 3rd bar
        self.assertEqual(pd.Timestamp('2019-01-01 00:04:00'), res[8][0])
        assert_array_equal([[1.0, 2.0], [1, 2], [1, np.nan]], res[8][1]['open'])
        assert_array_equal([[1.1, 2], [1, 2], [1, np.nan]], res[8][1]['high'])
        assert_array_equal([[0.9, 1.9], [1, 2], [1, np.nan]], res[8][1]['low'])
        assert_array_equal([[1, 1.9], [1, 2], [1, np.nan]], res[8][1]['close'])

        # 4rd bar
        self.assertEqual([None] * 4, res[9: 13])
        self.assertEqual(pd.Timestamp('2019-01-01 00:05:00'), res[13][0])
        assert_array_equal([[1.0, 2.0], [1, 2], [1, np.nan], [1.3, 2]], res[13][1]['open'])
        assert_array_equal([[1.1, 2], [1, 2], [1, np.nan], [1.3, 2.4]], res[13][1]['high'])
        assert_array_equal([[0.9, 1.9], [1, 2], [1, np.nan], [1.3, 1.8]], res[13][1]['low'])
        assert_array_equal([[1, 1.9], [1, 2], [1, np.nan], [1.3, 2.4]], res[13][1]['close'])

        # 5th bar
        self.assertEqual(pd.Timestamp('2019-01-01 00:06:00'), res[14][0])
        assert_array_equal([[1.0, 2.0], [1, 2], [1, np.nan], [1.3, 2], [1, np.nan]], res[14][1]['open'])
        assert_array_equal([[1.1, 2], [1, 2], [1, np.nan], [1.3, 2.4], [1, np.nan]], res[14][1]['high'])
        assert_array_equal([[0.9, 1.9], [1, 2], [1, np.nan], [1.3, 1.8], [1, np.nan]], res[14][1]['low'])
        assert_array_equal([[1, 1.9], [1, 2], [1, np.nan], [1.3, 2.4], [1, np.nan]], res[14][1]['close'])

        # 6th bar
        # we test time idx here since it's the final one
        self.assertEqual(pd.Timestamp('2019-01-01 00:07:00'), res[15][0])
        assert_array_equal([[1, 2], [1, np.nan], [1.3, 2], [1, np.nan], [np.nan, 2]], res[15][1]['open'])
        assert_array_equal([[1, 2], [1, np.nan], [1.3, 2.4], [1, np.nan], [np.nan, 2]], res[15][1]['high'])
        assert_array_equal([[1, 2], [1, np.nan], [1.3, 1.8], [1, np.nan], [np.nan, 2]], res[15][1]['low'])
        assert_array_equal([[1, 2], [1, np.nan], [1.3, 2.4], [1, np.nan], [np.nan, 2]], res[15][1]['close'])


class TestMT4DataObject(unittest.TestCase):
    def test_mt4(self):
        async def client():
            # put data object inside client so that socket will be closed properly after going out of scope
            data_obj = MT4DataObject(['usd', 'eur'], 12345)
            data_obj.set_params(Freq.TICK, 4)

            res = []
            counter = 0
            async for data in data_obj():
                res.append(data)
                counter += 1
                if counter > 4:
                    break

            return res

        async def server():
            context = azmq.Context()
            # noinspection PyUnresolvedReferences
            socket = context.socket(zmq.PUB)
            socket.bind('tcp://127.0.0.1:12345')

            data = [
                'usd 1;1.1',
                'eur 2;2.05',
                'usd 1.1;1.15',
                'usd 1.15;1.2',
                'eur 2.05;2.1',
                'eur 2.1;2.1',
            ]

            await asyncio.sleep(1)  # let client to warm up
            print('server starts')
            for msg in data:
                await socket.send_string(msg)
                await asyncio.sleep(0.1)
            print('server finishes')

        async def main():
            res, _ = await asyncio.gather(client(), server())
            return res

        result = asyncio.run(main())

        self.assertEqual(5, len(result))

        # 1st bar
        assert_array_equal([[1.05, 2.025]], result[0][1]['open'])

        # 2nd bar
        self.assertTrue(result[0][0] < result[1][0])
        assert_array_equal([[1.05, 2.025], [1.125, np.nan]], result[1][1]['open'])

        # 3rd bar
        self.assertTrue(result[1][0] < result[2][0])
        assert_array_almost_equal([[1.05, 2.025], [1.125, np.nan], [1.175, np.nan]], result[2][1]['open'], decimal=10)

        # 4th bar
        self.assertTrue(result[2][0] < result[3][0])
        assert_array_almost_equal([[1.05, 2.025], [1.125, np.nan], [1.175, np.nan], [np.nan, 2.075]],
                                  result[3][1]['open'], decimal=10)

        # 5th bar
        self.assertTrue(result[3][0] < result[4][0])
        assert_array_almost_equal([[1.125, np.nan], [1.175, np.nan], [np.nan, 2.075], [np.nan, 2.1]],
                                  result[4][1]['open'], decimal=10)


class TestDataStore(unittest.TestCase):
    def test_table_creation(self):
        store = DataStore(Broker.MOCK, Freq.MINUTE)  # tick is not used and for testing purpose

        async def main():
            await store.connect()
            await store.disconnect()

        asyncio.run(main())

        database = Config['postgres_db']
        user = Config['postgres_user']
        password = Config['postgres_pass']
        port = Config['postgres_port']
        con = pg.connect(database=database, user=user, password=password, host='127.0.0.1', port=port)
        cur = con.cursor()
        cur.execute("select table_name from information_schema.tables where table_schema = 'public'")
        tables = [item[0] for item in cur.fetchall()]
        self.assertIn(store.table_name, tables)
        cur.execute(f'drop table {store.table_name}')
        con.commit()
        con.close()

    def test_insertion(self):
        asset = Asset('FX:ABC')
        store = DataStore(Broker.MOCK, Freq.MINUTE)  # tick is not used and for testing purpose
        now = datetime.utcnow().replace(tzinfo=timezone('UTC'))

        async def main():
            await store.connect()
            for i in range(5):
                data = Datum(asset, now + timedelta(minutes=i), 1, 2, 3, 4)
                await store.append(data)
            await store.disconnect()

        asyncio.run(main())

        database = Config['postgres_db']
        user = Config['postgres_user']
        password = Config['postgres_pass']
        port = Config['postgres_port']
        con = pg.connect(database=database, user=user, password=password, host='127.0.0.1', port=port)
        cur = con.cursor()
        cur.execute(f'select * from {store.table_name}')
        res = cur.fetchall()
        self.assertTupleEqual(res[0][2:], (1.0, 2.0, 3.0, 4.0))
        self.assertEqual(res[0][1].astimezone(timezone('EST')), now.astimezone(timezone('EST')))
        cur.execute(f'drop table {store.table_name}')
        con.commit()
        con.close()


class TestDownSampledBar(unittest.TestCase):
    def test_start(self):
        now = pd.Timestamp.now()
        print(now)
        data = Datum(Asset('FX:EURUSD'), now, 1, 2, 3, 4)
        bar = DownSampledBar(const.Freq.MINUTE, const.Freq.SECOND5)
        out = bar.update(data)
        self.assertIsNone(out)
        self.assertEqual(bar.timestamp, now.ceil('1min'))
        self.assertEqual(bar.check, now.ceil('1min'))
        self.assertEqual(bar.open, 1)
        self.assertEqual(bar.close, 4)

        bar = DownSampledBar(const.Freq.MINUTE, const.Freq.SECOND5, 2)
        bar.update(data)
        self.assertEqual(bar.timestamp, now.ceil('1min'))
        self.assertEqual(bar.check, now.ceil('1min') - pd.Timedelta(seconds=10))
        self.assertEqual(bar.high, 2)

    def test_update(self):
        asset = Asset('FX:EURUSD')
        now = pd.Timestamp('2019-01-01 12:01:00')
        delta = pd.Timedelta(seconds=5)
        data = Datum(asset, now, 1, 2, 3, 4)
        bar = DownSampledBar(const.Freq.MINUTE, const.Freq.SECOND5)

        # test immediate return
        out = bar.update(data)
        self.assertIsNotNone(out)
        self.assertTupleEqual(out, (now, 1, 2, 3, 4))

        # test regular update
        self.assertIsNone(bar.update(Datum(asset, pd.Timestamp('2019-01-01 12:01:05'), 1, 3, 0, 2)))
        self.assertIsNone(bar.update(Datum(asset, pd.Timestamp('2019-01-01 12:01:55'), 1, 6, 0, 2)))
        out = bar.update(Datum(asset, pd.Timestamp('2019-01-01 12:02:00'), 2, 5, 1, 1))
        self.assertEqual(out, (pd.Timestamp('2019-01-01 12:02:00'), 1, 6, 0, 1))
        self.assertEqual(bar.check, pd.Timestamp.min)
        self.assertEqual(bar.open, None)

        # test consecutive update
        self.assertIsNone(bar.update(Datum(asset, pd.Timestamp('2019-01-01 12:02:05'), 1, 3, 0, 2)))
        self.assertIsNone(bar.update(Datum(asset, pd.Timestamp('2019-01-01 12:02:10'), 4, 5, -1, 3)))
        out = bar.update(Datum(asset, pd.Timestamp('2019-01-01 12:03:00'), 2, 4, 1, 1))
        self.assertEqual(out, (pd.Timestamp('2019-01-01 12:03:00'), 1, 5, -1, 1))

        # test skip update
        self.assertIsNone(bar.update(Datum(asset, pd.Timestamp('2019-01-01 12:04:05'), 1, 3, 0, 2)))  # skip from the 3rd minute
        out = bar.update(Datum(asset, pd.Timestamp('2019-01-01 12:05:05'), 2, 4, 1, 1))  # skip from the 4th minute
        self.assertTupleEqual(out, (pd.Timestamp('2019-01-01 12:05:00'), 1, 3, 0, 2))
        self.assertEqual(bar.timestamp, pd.Timestamp('2019-01-01 12:06:00'))
        self.assertEqual(bar.open, 2)
        self.assertEqual(bar.high, 4)
        self.assertEqual(bar.close, 1)

    def test_offset(self):
        asset = Asset('FX:EURUSD')
        now = pd.Timestamp('2019-01-01 12:00:55')  # start from the last bar
        delta = pd.Timedelta(seconds=5)
        bar = DownSampledBar(const.Freq.MINUTE, const.Freq.SECOND5, offset=1)

        # test start
        out = bar.update(Datum(asset, now, 1, 3, 0, 2))
        self.assertTupleEqual(out, (pd.Timestamp('2019-01-01 12:01:00'), 1, 3, 0, 2))

        # test update
        self.assertIsNone(bar.update(Datum(asset, pd.Timestamp('2019-01-01 12:01:00'), 1, 3, 0, 2)))
        out = bar.update(Datum(asset, pd.Timestamp('2019-01-01 12:01:55'), 6, 9, 4, 5))
        self.assertTupleEqual(out, (pd.Timestamp('2019-01-01 12:02:00'), 1, 9, 0, 5))
        self.assertEqual(bar.check, pd.Timestamp.min)
        self.assertIsNone(bar.open)

        # test skip
        self.assertIsNone(bar.update(Datum(asset, pd.Timestamp('2019-01-01 12:03:00'), 6, 9, 4, 5)))
        out = bar.update(Datum(asset, pd.Timestamp('2019-01-01 12:04:00'), 1, 3, 0, 2))
        self.assertEqual(out, (pd.Timestamp('2019-01-01 12:04:00'), 6, 9, 4, 5))
        self.assertEqual(bar.timestamp, pd.Timestamp('2019-01-01 12:05:00'))
        self.assertEqual(bar.open, 1)
        self.assertEqual(bar.close, 2)

    def test_5minute(self):
        asset = Asset('FX:EURUSD')
        delta = pd.Timedelta(seconds=5)
        bar = DownSampledBar(const.Freq.MINUTE5, const.Freq.SECOND5, offset=1)

        # test start
        out = bar.update(Datum(asset, pd.Timestamp('2019-01-01 12:04:55'), 1, 3, 0, 2))
        self.assertTupleEqual(out, (pd.Timestamp('2019-01-01 12:05:00'), 1, 3, 0, 2))

        # test update
        self.assertIsNone(bar.update(Datum(asset, pd.Timestamp('2019-01-01 12:05:00'), 1, 3, 0, 2)))
        out = bar.update(Datum(asset, pd.Timestamp('2019-01-01 12:09:55'), 6, 9, 4, 5))
        self.assertTupleEqual(out, (pd.Timestamp('2019-01-01 12:10:00'), 1, 9, 0, 5))
        self.assertEqual(bar.check, pd.Timestamp.min)
        self.assertIsNone(bar.open)

        # test skip
        self.assertIsNone(bar.update(Datum(asset, pd.Timestamp('2019-01-01 12:10:00'), 6, 9, 4, 5)))
        out = bar.update(Datum(asset, pd.Timestamp('2019-01-01 12:15:00'), 1, 3, 0, 2))
        self.assertEqual(out, (pd.Timestamp('2019-01-01 12:15:00'), 6, 9, 4, 5))
        self.assertEqual(bar.timestamp, pd.Timestamp('2019-01-01 12:20:00'))
        self.assertEqual(bar.open, 1)
        self.assertEqual(bar.close, 2)


if __name__ == '__main__':
    unittest.main()
