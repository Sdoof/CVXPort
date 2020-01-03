
import unittest
import threading
import numpy as np
import pandas as pd
import asyncio
from typing import List
import time

from cvxport import JobError, Asset, const
from cvxport.executor import Executor
from cvxport.strategy import Strategy
from cvxport.worker import schedulable, service
from cvxport.data_server import DataServer
from cvxport.data import Datum
from cvxport.controller import Controller


class MockStrategy(Strategy):
    def __init__(self):
        super(MockStrategy, self).__init__('mock_strategy', [Asset('STK:AAPL'), Asset('STK:TSLA')])
        self.sign = 1

    def _generate_positions(self, data: dict):
        self.sign *= -1
        if self.sign < 0:
            return np.array([-10, -10])
        return np.array([20, 0])


class MockedExecutor(Executor):
    @schedulable()
    async def kill(self):
        await asyncio.sleep(5)
        raise JobError('Killed')


class MockDataServer(DataServer):
    def __init__(self):
        super().__init__(const.Broker.MOCK)

    async def subscribe(self, assets: List[Asset]):
        pass

    @service()
    async def emit_data(self):
        for asset in self.subscribed:
            await self.data_queue.put(Datum(asset, pd.Timestamp.now(), 2, 3, 0, 1))
        await asyncio.sleep(0.5)

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
        def run_executor():
            time.sleep(0.5)
            executor = MockedExecutor(MockStrategy(), const.Broker.MOCK, 10000)
            executor.run()

        def run_data_server():
            server = MockDataServer()
            server.run()

        def run_controller():
            controller = MockedController()
            controller.run()





if __name__ == '__main__':
    unittest.main()