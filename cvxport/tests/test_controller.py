
import unittest
import zmq
import time
from datetime import datetime
import threading
import asyncio
import pandas as pd

from cvxport.controller import Controller, schedulable
from cvxport import JobError


class MockedController(Controller):
    @schedulable()
    async def shutdown(self):
        await asyncio.sleep(3)
        raise JobError("Time's up")


class TestController(unittest.TestCase):
    def test_registration(self):
        # need to use different name because logger persist globally
        # otherwise, the next test will use the same logger and we will output to the old log file too!
        controller = MockedController('con1')
        port = controller.current_usable_port

        results = [0] * 5
        clock = [datetime.now()]

        def mock_worker1():
            """
            proper worker without error
            """
            context = zmq.Context()
            # noinspection PyUnresolvedReferences
            socket = context.socket(zmq.REQ)
            socket.connect(f'tcp://127.0.0.1:{controller.controller_port}')
            socket.send_string('worker1|3')
            results[0] = int(socket.recv_string())  # should get controller_port
            clock[0] = pd.Timestamp.now('EST')  # to check if registration of worker1 gets overridden

        def mock_worker2():
            """
            proper worker without error. To check if starting port is moved
            """
            time.sleep(0.5)  # so that worker1 will run before worker2
            context = zmq.Context()
            # noinspection PyUnresolvedReferences
            socket = context.socket(zmq.REQ)
            socket.connect(f'tcp://127.0.0.1:{controller.controller_port}')
            socket.send_string('worker2|5')
            results[1] = int(socket.recv_string())  # should get controller_port + 3

        def mock_worker3():
            """
            submit name of worker1 to create duplication
            """
            time.sleep(1)  # so that worker1 will run before worker3
            context = zmq.Context()
            # noinspection PyUnresolvedReferences
            socket = context.socket(zmq.REQ)
            socket.connect(f'tcp://127.0.0.1:{controller.controller_port}')
            socket.send_string('worker1|3')
            results[2] = int(socket.recv_string())  # should get -1

        def mock_worker4():
            """
            test registration lost
            """
            context = zmq.Context()
            # noinspection PyUnresolvedReferences
            socket = context.socket(zmq.REQ)
            socket.connect(f'tcp://127.0.0.1:{controller.controller_port}')
            socket.send_string('worker4')
            results[3] = int(socket.recv_string())  # should get -1 because we didn't register

        def mock_worker5():
            """
            improper format
            """
            context = zmq.Context()
            # noinspection PyUnresolvedReferences
            socket = context.socket(zmq.REQ)
            socket.connect(f'tcp://127.0.0.1:{controller.controller_port}')
            socket.send_string('worker5|5|')
            results[4] = int(socket.recv_string())  # should get -1 because of format

        threads = [threading.Thread(target=worker)
                   for worker in [mock_worker1, mock_worker2, mock_worker3, mock_worker4, mock_worker5]]
        [t.start() for t in threads]

        controller.run()

        [t.join() for t in threads]
        self.assertListEqual(results, [port, port + 3, -1, -1, -1])
        self.assertLessEqual(controller.registry['worker1'], clock[0])
        self.assertSetEqual(set(controller.registry.keys()), {'worker1', 'worker2'})

    def test_heartbeat_handling(self):
        controller = MockedController('con2')
        controller.heartbeat_interval = 0.9
        result1 = []
        result2 = []

        def mock_worker1():
            """
            proper worker without error
            """
            context = zmq.Context()
            # noinspection PyUnresolvedReferences
            socket = context.socket(zmq.REQ)
            socket.connect(f'tcp://127.0.0.1:{controller.controller_port}')

            # register
            socket.send_string('worker1|3')
            socket.recv_string()

            for _ in range(3):
                time.sleep(0.8)
                socket.send_string('worker1')
                result1.append(int(socket.recv_string()))

        def mock_worker2():
            """
            stop sending heartbeat in the middle
            """
            context = zmq.Context()
            # noinspection PyUnresolvedReferences
            socket = context.socket(zmq.REQ)
            socket.connect(f'tcp://127.0.0.1:{controller.controller_port}')

            # register
            socket.send_string('worker2|3')
            socket.recv_string()

            time.sleep(0.8)
            socket.send_string('worker2')
            result2.append(int(socket.recv_string()))

            time.sleep(1.6)
            socket.send_string('worker2')
            result2.append(int(socket.recv_string()))

        threads = [threading.Thread(target=worker) for worker in [mock_worker1, mock_worker2]]
        [t.start() for t in threads]

        controller.run()

        [t.join() for t in threads]
        self.assertListEqual(result1, [0, 0, 0])
        self.assertListEqual(result2, [0, -1])
        self.assertSetEqual(set(controller.registry.keys()), {'worker1'})


if __name__ == '__main__':
    unittest.main()