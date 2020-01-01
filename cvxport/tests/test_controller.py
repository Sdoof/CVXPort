
import unittest
import zmq
import time
from datetime import datetime
import threading
import asyncio
import pandas as pd

from cvxport.controller import Controller, schedulable
from cvxport import JobError, const


class MockedController(Controller):
    @schedulable()
    async def kill(self):
        await asyncio.sleep(3)
        raise JobError("Time's up")


class TestController(unittest.TestCase):
    def test_registration(self):
        # need to use different name because logger persist globally
        # otherwise, the next test will use the same logger and we will output to the old log file too!
        controller = MockedController('con1')
        port = controller.current_usable_port

        results = [0] * 4
        clock = [datetime.now()]

        def mock_worker1():
            """
            proper worker without error
            """
            context = zmq.Context()
            # noinspection PyUnresolvedReferences
            socket = context.socket(zmq.REQ)
            socket.connect(f'tcp://127.0.0.1:{controller.port_map["controller_port"]}')
            socket.send_string('worker1|fake_port')
            results[0] = socket.recv_json()  # should get {'fake_port': 'controller_port'}
            clock[0] = pd.Timestamp.now('EST')  # to check if registration of worker1 gets overridden

        def mock_worker2():
            """
            proper worker without error. To check if starting port is moved
            """
            time.sleep(1)  # so that worker1 will run before worker2
            context = zmq.Context()
            # noinspection PyUnresolvedReferences
            socket = context.socket(zmq.REQ)
            socket.connect(f'tcp://127.0.0.1:{controller.port_map["controller_port"]}')
            socket.send_string('worker2|fake_port1|fake_port2')
            results[1] = socket.recv_json()  # should get controller_port + 1, + 2

        def mock_worker3():
            """
            submit name of worker1 to create duplication
            """
            time.sleep(1)  # so that worker1 will run before worker3
            context = zmq.Context()
            # noinspection PyUnresolvedReferences
            socket = context.socket(zmq.REQ)
            socket.connect(f'tcp://127.0.0.1:{controller.port_map["controller_port"]}')
            socket.send_string('worker1|fake_port')
            results[2] = socket.recv_json()  # should get -1

        def mock_worker4():
            """
            test registration lost
            """
            context = zmq.Context()
            # noinspection PyUnresolvedReferences
            socket = context.socket(zmq.REQ)
            socket.connect(f'tcp://127.0.0.1:{controller.port_map["controller_port"]}')
            socket.send_string('worker4')
            results[3] = socket.recv_json()

        threads = [threading.Thread(target=worker)
                   for worker in [mock_worker1, mock_worker2, mock_worker3, mock_worker4]]
        [t.start() for t in threads]

        controller.run()

        [t.join() for t in threads]
        self.assertListEqual(results, [{'fake_port': port},
                                       {'fake_port1': port + 1, 'fake_port2': port + 2},
                                       {'code': const.CCode.AlreadyRegistered.value},
                                       {'code': const.CCode.NotInRegistry.value}])

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
            socket.connect(f'tcp://127.0.0.1:{controller.port_map["controller_port"]}')

            # register
            socket.send_string('worker1|3')
            socket.recv_string()

            for _ in range(3):
                time.sleep(0.8)
                socket.send_string('worker1')
                result1.append(socket.recv_json())

        def mock_worker2():
            """
            stop sending heartbeat in the middle
            """
            context = zmq.Context()
            # noinspection PyUnresolvedReferences
            socket = context.socket(zmq.REQ)
            socket.connect(f'tcp://127.0.0.1:{controller.port_map["controller_port"]}')

            # register
            socket.send_string('worker2|3')
            socket.recv_string()

            time.sleep(0.8)
            socket.send_string('worker2')
            result2.append(socket.recv_json())

            time.sleep(1.6)
            socket.send_string('worker2')
            result2.append(socket.recv_json())

        threads = [threading.Thread(target=worker) for worker in [mock_worker1, mock_worker2]]
        [t.start() for t in threads]

        controller.run()

        print(result1)
        print(result2)

        [t.join() for t in threads]
        self.assertListEqual(result1, [{'code': const.CCode.Succeeded.value}] * 3)
        self.assertListEqual(result2,
                             [{'code': const.CCode.Succeeded.value}, {'code': const.CCode.NotInRegistry.value}])
        self.assertSetEqual(set(controller.registry.keys()), {'worker1'})

    def test_data_server_registration(self):
        controller = MockedController('con3')
        port = controller.current_usable_port
        results = {}

        def mock_data_server1():
            """
            data server with missing port
            """
            context = zmq.Context()
            # noinspection PyUnresolvedReferences
            socket = context.socket(zmq.REQ)
            socket.connect(f'tcp://127.0.0.1:{controller.port_map["controller_port"]}')
            socket.send_string('DataServer:IB|subscription_port|fake_port')
            results['ds1'] = socket.recv_json()  # should get -2

        def mock_data_server2():
            """
            data server with unknown broker name
            """
            time.sleep(0.5)  # so that run after server1
            context = zmq.Context()
            # noinspection PyUnresolvedReferences
            socket = context.socket(zmq.REQ)
            socket.connect(f'tcp://127.0.0.1:{controller.port_map["controller_port"]}')
            socket.send_string('DataServer:ABC|subscription_port|broadcast_port|fake_port')
            results['ds2'] = socket.recv_json()  # should get -5

        def mock_data_server3():
            """
            proper data server without error
            """
            time.sleep(1)  # so that run after server 1 and 2
            context = zmq.Context()
            # noinspection PyUnresolvedReferences
            socket = context.socket(zmq.REQ)
            socket.connect(f'tcp://127.0.0.1:{controller.port_map["controller_port"]}')
            socket.send_string('DataServer:IB|subscription_port|broadcast_port|fake_port')
            results['ds3'] = socket.recv_json()  # should get assignment of the 3 ports

        def mock_data_server4():
            """
            duplicated data server
            """
            time.sleep(1.5)  # so that run after server 1 and 2
            context = zmq.Context()
            # noinspection PyUnresolvedReferences
            socket = context.socket(zmq.REQ)
            socket.connect(f'tcp://127.0.0.1:{controller.port_map["controller_port"]}')
            socket.send_string('DataServer:IB|subscription_port|broadcast_port|fake_port')
            results['ds4'] = socket.recv_json()  # should get -1

        threads = [threading.Thread(target=worker)
                   for worker in [mock_data_server1, mock_data_server2, mock_data_server3, mock_data_server4]]
        [t.start() for t in threads]

        controller.run()

        [t.join() for t in threads]
        self.assertDictEqual(results['ds1'], {'code': const.CCode.MissingRequiredPort.value})
        self.assertDictEqual(results['ds2'], {'code': const.CCode.UnKnownBroker.value})
        self.assertDictEqual(results['ds4'], {'code': const.CCode.AlreadyRegistered.value})

        self.assertSetEqual(set(results['ds3'].keys()), {'subscription_port', 'broadcast_port', 'fake_port'})
        self.assertDictEqual(results['ds3'], controller.data_servers['IB'])
        self.assertEqual(sum(results['ds3'].values()), 3 * port + 3)

    def test_registration_without_port(self):
        controller = MockedController('con4')
        results = {}

        def mock_worker():
            """
            data server without port request
            """
            context = zmq.Context()
            # noinspection PyUnresolvedReferences
            socket = context.socket(zmq.REQ)
            socket.connect(f'tcp://127.0.0.1:{controller.port_map["controller_port"]}')
            socket.send_string('Worker|')
            results['worker'] = socket.recv_json()  # should get -2

        t = threading.Thread(target=mock_worker)
        t.start()

        controller.run()

        t.join()
        self.assertDictEqual(results['worker'], {})


if __name__ == '__main__':
    unittest.main()
