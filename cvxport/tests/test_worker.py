
import asyncio
import unittest
import time
import threading
import zmq

from cvxport.worker import SatelliteWorker, service, schedulable
from cvxport.controller import Controller
from cvxport import const, JobError, Config


# mock class
class MockedWorker(SatelliteWorker):
    @schedulable()
    async def kill(self):
        await asyncio.sleep(3)
        raise JobError('Killed')


class MockedWorker2(MockedWorker):
    @schedulable(socket='dummy_port|REP')
    async def dummy_job(self, socket):
        await asyncio.sleep(0)

    @service(socket='dummy_port2|REP')
    async def dummy_job2(self, socket):
        await asyncio.sleep(0)


class MockedController(Controller):
    @schedulable()
    async def kill(self):
        await asyncio.sleep(3)
        raise JobError('Killed')


class TestSatelliteWorker(unittest.TestCase):
    def test_registration_timeout(self):
        """
        Test if controller registration will time out as expected
        """
        worker = MockedWorker('test')
        worker.wait_time = 1.5  # 1.5 second

        start = time.time()

        with self.assertRaises(JobError) as cm:
            asyncio.run(worker._run())
        self.assertEqual(str(cm.exception), 'Controller registration times out')

        duration = time.time() - start
        # since we set the wait time to be 1.5 second, the worker should return between 1.5 to 1.6 seconds
        self.assertGreater(duration, 1.5)
        self.assertLess(duration, 1.6)

    def test_registration_rejection(self):
        """
        Test if controller registration is rejected
        """
        worker = MockedWorker('test')

        def mock_controller():
            context = zmq.Context()
            # noinspection PyUnresolvedReferences
            socket = context.socket(zmq.REP)
            socket.bind(f'tcp://127.0.0.1:{worker.port_map["controller_port"]}')
            socket.recv_string()
            socket.send_json({'code': const.CCode.AlreadyRegistered.value})

        # start controller
        t = threading.Thread(target=mock_controller)
        t.start()

        start = time.time()

        with self.assertRaises(JobError) as cm:
            asyncio.run(worker._run())
        self.assertEqual(str(cm.exception), 'AlreadyRegistered')

        duration = time.time() - start
        t.join()
        # This test should return almost immediately
        self.assertLess(duration, 0.5)

    def test_registration_and_heartbeat(self):
        worker = MockedWorker2('test')
        worker.heartbeat_interval = 0.5
        worker.wait_time = 0.2
        starting_port = worker.port_map["controller_port"]

        messages = []

        def mock_controller():
            context = zmq.Context()
            # noinspection PyUnresolvedReferences
            socket = context.socket(zmq.REP)
            socket.bind(f'tcp://127.0.0.1:{worker.port_map["controller_port"]}')

            # mock registration
            messages.append(socket.recv_json())
            socket.send_json({'dummy_port': starting_port + 1, 'dummy_port2': starting_port + 2})

            # mock heartbeat
            for _ in range(4):
                messages.append(socket.recv_json())
                socket.send_json({'code': const.CCode.Succeeded.value})

        t = threading.Thread(target=mock_controller)
        t.start()
        start = time.time()

        with self.assertRaises(JobError) as cm:
            asyncio.run(worker._run())
        self.assertEqual(str(cm.exception), 'Heartbeat times out')

        duration = time.time() - start
        t.join()
        self.assertGreater(duration, 2)  # 4 heartbeats take 2s
        self.assertLess(duration, 2.5)  # wait time add 0.2, so the process should take around 2.2s
        self.assertListEqual(messages,
                             [{'name': 'test', 'type': 'register', 'ports': ['dummy_port', 'dummy_port2']}] +
                             [{'name': 'test', 'type': 'hb'}] * 4)
        self.assertDictEqual(worker.port_map, {'controller_port': starting_port,
                                               'dummy_port': starting_port + 1,
                                               'dummy_port2': starting_port + 2})

    def test_registration_lost(self):
        worker = MockedWorker('test')
        worker.heartbeat_interval = 0.5
        worker.wait_time = 0.2

        def mock_controller():
            context = zmq.Context()
            # noinspection PyUnresolvedReferences
            socket = context.socket(zmq.REP)
            socket.bind(f'tcp://127.0.0.1:{worker.port_map["controller_port"]}')
            socket.recv_string()
            socket.send_json({})  # MockedWorker doesn't need port assignment
            socket.recv_string()
            socket.send_json({'code': const.CCode.NotInRegistry.value})  # mock registration lost

        t = threading.Thread(target=mock_controller)
        t.start()

        with self.assertRaises(JobError) as cm:
            asyncio.run(worker._run())
        self.assertEqual(str(cm.exception), 'NotInRegistry')

        t.join()

    def test_controller_interaction(self):
        agents = {}

        def run_worker():
            time.sleep(0.2)
            worker = MockedWorker2('test')
            worker.heartbeat_interval = 0.5
            worker.wait_time = 0.2
            agents['worker'] = worker
            worker.run()

        def run_controller():
            controller = MockedController()
            controller.heartbeat_interval = 0.5
            agents['controller'] = controller
            controller.run()

        threads = [threading.Thread(target=func) for func in [run_controller, run_worker]]
        [t.start() for t in threads]
        [t.join() for t in threads]

        start_port = Config['starting_port']
        con_port = Config['controller_port']
        self.assertIn('test', agents['controller'].registry)
        self.assertEqual(agents['worker'].port_map,
                         {'controller_port': con_port, 'dummy_port': start_port, 'dummy_port2': start_port + 1})



if __name__ == '__main__':
    unittest.main()
