
import unittest
import asyncio
from typing import List
import threading
import zmq
import time
import psycopg2 as pg
import pandas as pd

from cvxport import const, JobError, Config, utils
from cvxport.data import Asset, Datum
from cvxport.data_server import DataServer
from cvxport.controller import Controller
from cvxport.worker import service, schedulable


class MockDataServer(DataServer):
    def __init__(self):
        super().__init__(const.Broker.MOCK)
        self.check = []

    async def subscribe(self, assets: List[Asset]):
        self.check += assets  # for testing double subscription

    async def execute(self, name: str, order: dict) -> dict:
        return {asset.string: [0, 100, 0] for asset in order}

    @service()
    async def emit_data(self):
        for asset in self.subscribed:
            await self.data_queue.put(Datum(asset, pd.Timestamp.utcnow(), 2, 3, 0, 1))
        await asyncio.sleep(0.5)

    @schedulable()
    async def kill(self):
        await asyncio.sleep(3)
        raise JobError("Time's up")


class MockedController(Controller):
    @schedulable()
    async def kill(self):
        await asyncio.sleep(3)
        raise JobError("Time's up")


class TestDataServer(unittest.TestCase):
    def test_controller_communication(self):
        results = {}

        def start_controller():
            controller = MockedController()
            controller.run()

        def start_data_server():
            server = MockDataServer()
            results['subscribed'] = server.check
            server.run()

        def subscriber():
            time.sleep(1)  # wait for controller and data server to be ready
            context = zmq.Context()

            # get ports
            # noinspection PyUnresolvedReferences
            socket = context.socket(zmq.REQ)
            socket.connect(f'tcp://127.0.0.1:{Config["controller_comm_port"]}')
            socket.send_string('DataServer:MOCK')
            ports = socket.recv_json()
            results['ports'] = ports
            socket.close()

            # order data
            # noinspection PyUnresolvedReferences
            socket = context.socket(zmq.REQ)
            socket.connect(f'tcp://127.0.0.1:{ports["subscription_port"]}')
            socket.send_string('FX:EURUSD,STK:AAPL')
            results['ret'] = socket.recv_json()

            # try to double subscribe
            socket.send_string('FX:EURUSD')
            socket.recv_json()

            socket.close()

            # subscribe to data
            # noinspection PyUnresolvedReferences
            socket = context.socket(zmq.SUB)
            [socket.subscribe(name) for name in ['FX:EURUSD', 'STK:AAPL']]
            socket.connect(f'tcp://127.0.0.1:{ports["data_port"]}')
            msgs = []
            for _ in range(4):
                msgs.append(socket.recv_string())
            socket.close()
            results['msgs'] = msgs

            # execute order
            socket = context.socket(zmq.REQ)
            socket.connect(f'tcp://127.0.0.1:{ports["order_port"]}')
            socket.send_json({'Strategy': 'MockStrategy', 'FX:EURUSD': -5})  # sell 5 shares
            results['executions'] = socket.recv_json()
            socket.close()

        threads = [threading.Thread(target=func) for func in [start_controller, start_data_server, subscriber]]
        [t.start() for t in threads]
        [t.join() for t in threads]

        # check ports
        self.assertEqual(len(results['ports']), 3)  # return exactly 2 ports
        self.assertDictEqual(results['ret'], {'code': const.DCode.Succeeded.value})

        # check double subscription
        self.assertEqual(len(results['subscribed']), len(utils.unique(results['subscribed'])))

        # check market data
        headers = [s.split(',')[0] for s in results['msgs']]
        lengths = [len(s.split(',')) for s in results['msgs']]
        self.assertSetEqual(set(headers), {'FX:EURUSD', 'STK:AAPL'})
        self.assertListEqual(lengths, [6] * 4)

        # check executions
        self.assertDictEqual(results['executions'], {'FX:EURUSD': [0, 100, 0]})

        # check data is written to database
        database = Config['postgres_db']
        user = Config['postgres_user']
        password = Config['postgres_pass']
        port = Config['postgres_port']
        con = pg.connect(database=database, user=user, password=password, host='127.0.0.1', port=port)
        cur = con.cursor()
        cur.execute(f'select count(*) from mock_minute5')
        res = cur.fetchall()
        self.assertGreater(res[0][0], 6)
        cur.execute(f'drop table mock_minute5')
        con.commit()
        con.close()


if __name__ == '__main__':
    unittest.main()
