
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

        results = {}
        clock = [datetime.now()]

        def mock_worker1():
            """
            proper worker without error
            """
            context = zmq.Context()
            # noinspection PyUnresolvedReferences
            socket = context.socket(zmq.REQ)
            socket.connect(f'tcp://127.0.0.1:{controller.port_map["controller_port"]}')
            socket.send_json({'name': 'worker1', 'type': 'register', 'ports': ['fake_port']})
            results['w1'] = socket.recv_json()  # should get {'fake_port': 'controller_port'}
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
            socket.send_json({'name': 'worker2', 'type': 'register', 'ports': ['fake_port1', 'fake_port2']})
            results['w2'] = socket.recv_json()  # should get controller_port + 1, + 2

        def mock_worker3():
            """
            submit name of worker1 to create duplication
            """
            time.sleep(1)  # so that worker1 will run before worker3
            context = zmq.Context()
            # noinspection PyUnresolvedReferences
            socket = context.socket(zmq.REQ)
            socket.connect(f'tcp://127.0.0.1:{controller.port_map["controller_port"]}')
            socket.send_json({'name': 'worker1', 'type': 'register', 'ports': ['fake_port']})
            results['w3'] = socket.recv_json()  # should get -1

        def mock_worker4():
            """
            test registration lost
            """
            context = zmq.Context()
            # noinspection PyUnresolvedReferences
            socket = context.socket(zmq.REQ)
            socket.connect(f'tcp://127.0.0.1:{controller.port_map["controller_port"]}')
            socket.send_json({'name': 'worker4', 'type': 'register'})
            results['w4'] = socket.recv_json()

        def mock_worker5():
            """
            missing name
            """
            context = zmq.Context()
            # noinspection PyUnresolvedReferences
            socket = context.socket(zmq.REQ)
            socket.connect(f'tcp://127.0.0.1:{controller.port_map["controller_port"]}')
            socket.send_json({'type': 'register'})
            results['w5'] = socket.recv_json()

        def mock_worker6():
            """
            missing type
            """
            context = zmq.Context()
            # noinspection PyUnresolvedReferences
            socket = context.socket(zmq.REQ)
            socket.connect(f'tcp://127.0.0.1:{controller.port_map["controller_port"]}')
            socket.send_json({'name': 'worker6'})
            results['w6'] = socket.recv_json()

        def mock_worker7():
            """
            test proper data server registration
            """
            time.sleep(1.5)  # to wait for worker2 so that we know what ports are assigned
            context = zmq.Context()
            # noinspection PyUnresolvedReferences
            socket = context.socket(zmq.REQ)
            socket.connect(f'tcp://127.0.0.1:{controller.port_map["controller_port"]}')
            socket.send_json({
                'name': 'DataServer:MOCK', 'type': 'register',
                'ports': ['subscription_port', 'data_port', 'order_port'],
                'info': {'freq': const.Freq.MINUTE5.value, 'offset': 1}
            })
            results['w7'] = socket.recv_json()

        def mock_worker8():
            """
            missing data server info
            """
            context = zmq.Context()
            # noinspection PyUnresolvedReferences
            socket = context.socket(zmq.REQ)
            socket.connect(f'tcp://127.0.0.1:{controller.port_map["controller_port"]}')
            socket.send_json({
                'name': 'DataServer:MOCK', 'type': 'register',
                'ports': ['subscription_port', 'data_port', 'order_port'],
            })
            results['w8'] = socket.recv_json()

        def mock_worker9():
            """
            test empty port in registration
            """
            context = zmq.Context()
            # noinspection PyUnresolvedReferences
            socket = context.socket(zmq.REQ)
            socket.connect(f'tcp://127.0.0.1:{controller.port_map["controller_port"]}')
            socket.send_json({'name': 'worker9', 'type': 'register', 'ports': []})
            results['w9'] = socket.recv_json()

        threads = [threading.Thread(target=worker)
                   for worker in [mock_worker1, mock_worker2, mock_worker3, mock_worker4, mock_worker5, mock_worker6,
                                  mock_worker7, mock_worker8, mock_worker9]]
        [t.start() for t in threads]

        controller.run()

        [t.join() for t in threads]
        self.assertEqual(results['w1'], {'fake_port': port})
        self.assertEqual(results['w2'], {'fake_port1': port + 1, 'fake_port2': port + 2})
        self.assertEqual(results['w3'], {'code': const.CCode.AlreadyRegistered.value})
        self.assertEqual(results['w4'], {'code': const.CCode.MissingRequiredPort.value})
        self.assertEqual(results['w5'], {'code': const.CCode.MissingName.value})
        self.assertEqual(results['w6'], {'code': const.CCode.UnknownRequest.value})

        self.assertEqual(results['w7'], {'subscription_port': port + 3, 'data_port': port + 4, 'order_port': port + 5})
        self.assertEqual(controller.data_servers['MOCK']['info'], {'freq': const.Freq.MINUTE5.value, 'offset': 1})

        self.assertEqual(results['w8'], {'code': const.CCode.MissingDataServerInfo.value})
        self.assertEqual(results['w9'], {})

        self.assertLessEqual(controller.registry['worker1'], clock[0])
        self.assertSetEqual(set(controller.registry.keys()), {'worker1', 'worker2', 'DataServer:MOCK', 'worker9'})

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
            socket.send_json({'name': 'worker1', 'type': 'register', 'ports': []})
            socket.recv_json()

            for _ in range(3):
                time.sleep(0.8)
                socket.send_json({'name': 'worker1', 'type': 'hb'})
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
            socket.send_json({'name': 'worker2', 'type': 'register', 'ports': []})
            socket.recv_json()

            time.sleep(0.8)
            socket.send_json({'name': 'worker2', 'type': 'hb'})
            result2.append(socket.recv_json())

            time.sleep(1.6)
            socket.send_json({'name': 'worker2', 'type': 'hb'})
            result2.append(socket.recv_json())

        threads = [threading.Thread(target=worker) for worker in [mock_worker1, mock_worker2]]
        [t.start() for t in threads]

        controller.run()

        [t.join() for t in threads]
        self.assertListEqual(result1, [{'code': const.CCode.Succeeded.value}] * 3)
        self.assertListEqual(result2,
                             [{'code': const.CCode.Succeeded.value}, {'code': const.CCode.NotInRegistry.value}])
        self.assertSetEqual(set(controller.registry.keys()), {'worker1'})

    def test_data_server_registration(self):
        """
        Seems to duplicate the first test. But it's fine testing more :)
        """
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
            socket.send_json({
                'name': 'DataServer:IB', 'type': 'register',
                'ports': ['subscription_port', 'fake_port'],
                'info': {'freq': const.Freq.HOURLY.value, 'offset': 0}
            })
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
            socket.send_json({
                'name': 'DataServer:ABC', 'type': 'register',
                'ports': ['subscription_port', 'data_port', 'order_port', 'fake_port'],
                'info': {'freq': const.Freq.HOURLY.value, 'offset': 0}
            })
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
            socket.send_json({
                'name': 'DataServer:IB', 'type': 'register',
                'ports': ['subscription_port', 'data_port', 'order_port', 'fake_port'],
                'info': {'freq': const.Freq.HOURLY.value, 'offset': 0}
            })
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
            socket.send_json({
                'name': 'DataServer:IB', 'type': 'register',
                'ports': ['subscription_port', 'data_port', 'order_port', 'fake_port'],
                'info': {'freq': const.Freq.HOURLY.value, 'offset': 0}
            })
            results['ds4'] = socket.recv_json()  # should get -1

        def mock_data_server5():
            """
            missing data server info
            """
            context = zmq.Context()
            # noinspection PyUnresolvedReferences
            socket = context.socket(zmq.REQ)
            socket.connect(f'tcp://127.0.0.1:{controller.port_map["controller_port"]}')
            socket.send_json({
                'name': 'DataServer:IB', 'type': 'register',
                'ports': ['subscription_port', 'data_port', 'order_port', 'fake_port'],
                'info': {'freq': const.Freq.HOURLY.value}
            })
            results['ds5'] = socket.recv_json()

        threads = [threading.Thread(target=worker)
                   for worker in [mock_data_server1, mock_data_server2, mock_data_server3,
                                  mock_data_server4, mock_data_server5]]
        [t.start() for t in threads]

        controller.run()

        [t.join() for t in threads]
        self.assertDictEqual(results['ds1'], {'code': const.CCode.MissingRequiredPort.value})
        self.assertDictEqual(results['ds2'], {'code': const.CCode.UnKnownBroker.value})
        self.assertDictEqual(results['ds4'], {'code': const.CCode.AlreadyRegistered.value})
        self.assertDictEqual(results['ds5'], {'code': const.CCode.MissingDataServerInfo.value})

        self.assertSetEqual(set(results['ds3'].keys()), {'subscription_port', 'data_port', 'order_port', 'fake_port'})
        self.assertDictEqual(results['ds3'], controller.data_servers['IB']['ports'])
        self.assertEqual(sum(results['ds3'].values()), 4 * port + 6)
        self.assertSetEqual(set(controller.data_servers.keys()), {'IB'})


if __name__ == '__main__':
    unittest.main()
