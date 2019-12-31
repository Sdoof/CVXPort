
import unittest
from datetime import datetime
import asyncio
from typing import List
import threading
import zmq
import time
import psycopg2 as pg

from cvxport import const, JobError, Config
from cvxport.data import Asset, Datum
from cvxport.data_server import DataServer
from cvxport.controller import Controller
from cvxport.worker import service, schedulable


class MockDataServer(DataServer):
    def __init__(self):
        super().__init__(const.Broker.MOCK)
        self.subscribed = {}

    async def subscribe(self, assets: List[Asset]):
        pass

    @service()
    async def emit_data(self):
        for asset in self.subscribed:
            await self.data_queue.put(Datum(asset, datetime.now(), 2, 3, 0, 1))
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
            server.run()

        def subscriber():
            time.sleep(1)  # wait for controller and data server to be ready
            context = zmq.Context()

            # get ports
            # noinspection PyUnresolvedReferences
            socket = context.socket(zmq.REQ)
            socket.connect(f'tcp://127.0.0.1:{Config["controller_comm_port"]}')
            socket.send_string('DataServer:MOCK')
            ports = eval(socket.recv_string())
            results['ports'] = ports
            socket.close()

            # order data
            # noinspection PyUnresolvedReferences
            socket = context.socket(zmq.REQ)
            socket.connect(f'tcp://127.0.0.1:{ports["subscription_port"]}')
            socket.send_string('FX:EURUSD,STK:AAPL')
            results['ret'] = eval(socket.recv_string())
            socket.close()

            # subscribe to data
            # noinspection PyUnresolvedReferences
            socket = context.socket(zmq.SUB)
            [socket.subscribe(name) for name in ['FX:EURUSD', 'STK:AAPL']]
            socket.connect(f'tcp://127.0.0.1:{ports["broadcast_port"]}')
            msgs = []
            for _ in range(4):
                msgs.append(socket.recv_string())
            socket.close()
            results['msgs'] = msgs

        threads = [threading.Thread(target=func) for func in [start_controller, start_data_server, subscriber]]
        [t.start() for t in threads]
        [t.join() for t in threads]

        self.assertEqual(len(results['ports']), 2)  # return exactly 2 ports
        self.assertDictEqual(results['ret'], {'code': const.DCode.Succeeded.value})
        headers = [s.split(',')[0] for s in results['msgs']]
        lengths = [len(s.split(',')) for s in results['msgs']]
        self.assertSetEqual(set(headers), {'FX:EURUSD', 'STK:AAPL'})
        self.assertListEqual(lengths, [6] * 4)

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
