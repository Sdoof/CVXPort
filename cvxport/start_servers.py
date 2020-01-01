
import threading
import zmq
from typing import Type
import asyncio
import time

from cvxport.controller import Controller
from cvxport.data_server import DataServer, IBDataServer
from cvxport import const, Config, JobError
from cvxport.data import Asset
from cvxport.worker import schedulable


def run_servers(data_server_class: Type[DataServer], broker: const.Broker, asset: Asset, duration: int = 20):
    results = {}

    # noinspection PyAbstractClass
    class MockedDataServer(data_server_class):
        @schedulable()
        async def kill(self):
            await asyncio.sleep(duration)
            raise JobError('Killed')

    class MockedController(Controller):
        @schedulable()
        async def kill(self):
            await asyncio.sleep(duration)
            raise JobError('Killed')

    def run_controller():
        controller = MockedController()
        controller.run()

    def run_data_server():
        # noinspection PyArgumentList
        server = MockedDataServer()
        server.run()

    def subscriber():
        time.sleep(1)  # wait for controller and data server to be ready
        context = zmq.Context()

        # get ports
        # noinspection PyUnresolvedReferences
        socket = context.socket(zmq.REQ)
        socket.connect(f'tcp://127.0.0.1:{Config["controller_comm_port"]}')
        socket.send_string(f'DataServer:{broker.name}')
        ports = eval(socket.recv_string())
        results['ports'] = ports
        socket.close()

        # order data
        # noinspection PyUnresolvedReferences
        socket = context.socket(zmq.REQ)
        socket.connect(f'tcp://127.0.0.1:{ports["subscription_port"]}')
        socket.send_string(asset.string)
        results['ret'] = eval(socket.recv_string())
        socket.close()

        # subscribe to data
        # noinspection PyUnresolvedReferences
        socket = context.socket(zmq.SUB)
        socket.subscribe(asset.string)
        socket.connect(f'tcp://127.0.0.1:{ports["broadcast_port"]}')
        msgs = []
        for _ in range(3):
            msgs.append(socket.recv_string())
        socket.close()
        results['msgs'] = msgs

    threads = [threading.Thread(target=func) for func in [run_data_server, run_controller, subscriber]]
    [t.start() for t in threads]
    [t.join() for t in threads]
    print(results)


if __name__ == '__main__':
    run_servers(IBDataServer, const.Broker.IB, Asset('FX:EURUSD'))
