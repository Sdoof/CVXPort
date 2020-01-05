
import unittest
import threading
import numpy as np
import pandas as pd
import asyncio
from typing import List, Dict
import time

from cvxport import JobError, Asset, const
from cvxport.executor import Executor
from cvxport.strategy import RandomStrategy
from cvxport.worker import schedulable, service
from cvxport.data_server import DataServer
from cvxport.data import Datum
from cvxport.controller import Controller


class MockedExecutor(Executor):
    @schedulable()
    async def kill(self):
        await asyncio.sleep(5)
        raise JobError('Killed')


class MockDataServer(DataServer):
    def __init__(self):
        super().__init__(const.Broker.MOCK, const.Freq.SECOND, offset=1)
        self.orders = []

    async def execute(self, name: str, order: Dict[Asset, int]) -> dict:
        self.orders.append(order)
        return {}

    async def subscribe(self, assets: List[Asset]):
        pass

    @service()
    async def emit_data(self):
        for asset in self.subscribed:
            await self.data_queue.put(Datum(asset, pd.Timestamp.utcnow(), 2, 3, 0, 1))
        await asyncio.sleep(1)

    @schedulable()
    async def kill(self):
        await asyncio.sleep(5)
        raise JobError("Killed")


class MockedController(Controller):
    @schedulable()
    async def kill(self):
        await asyncio.sleep(5)
        raise JobError("Killed")


class TestExecutor(unittest.TestCase):
    def test_startup(self):
        results = {}

        def run_executor():
            time.sleep(1)
            executor = MockedExecutor(
                RandomStrategy([Asset('STK:TSLA'), Asset('STK:AAPL')], const.Freq.SECOND, 1000),
                const.Broker.MOCK, 10000
            )
            executor.run()

        def run_data_server():
            server = MockDataServer()
            server.run()
            results['ds'] = server.orders

        def run_controller():
            controller = MockedController()
            controller.run()

        threads = [threading.Thread(target=func) for func in [run_executor, run_data_server, run_controller]]
        [t.start() for t in threads]
        [t.join() for t in threads]

        self.assertGreater(len(results['ds']), 2)


if __name__ == '__main__':
    unittest.main()