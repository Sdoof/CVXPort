
import threading
import zmq
from typing import Type
import asyncio
import time
import numpy as np

from cvxport.controller import Controller
from cvxport.data_server import DataServer, IBDataServer
from cvxport.executor import Executor
from cvxport.strategy import RandomStrategy, Strategy
from cvxport import const, Config, JobError
from cvxport.data import Asset
from cvxport.worker import schedulable


def run_servers(data_server_class: Type[DataServer], broker: const.Broker, strategy: Strategy, duration: int = 20):
    results = {}

    # noinspection PyAbstractClass
    # class MockedDataServer(data_server_class):
    #     @schedulable()
    #     async def kill(self):
    #         await asyncio.sleep(duration)
    #         raise JobError('Killed')
    #
    # class MockedController(Controller):
    #     @schedulable()
    #     async def kill(self):
    #         await asyncio.sleep(duration)
    #         raise JobError('Killed')

    def run_controller():
        controller = Controller()
        controller.run()

    def run_data_server():
        # noinspection PyArgumentList
        server = IBDataServer()
        server.run()

    def run_executor():
        time.sleep(1)  # wait for server to start
        executor = Executor(strategy, broker, 10000)
        executor.run()

    threads = [threading.Thread(target=func) for func in [run_data_server, run_controller, run_executor]]
    [t.start() for t in threads]
    [t.join() for t in threads]


if __name__ == '__main__':
    strat = RandomStrategy([Asset('FX:EURUSD')], const.Freq.MINUTE5, 10000)
    run_servers(IBDataServer, const.Broker.IB, strat, 600)
