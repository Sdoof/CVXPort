
import asyncio
import unittest
import time
import threading
import zmq

from cvxport.worker import SatelliteWorker, service, schedulable
from cvxport import const, JobError


# mock class
class MockedWorker(SatelliteWorker):
    @schedulable()
    async def kill(self):
        await asyncio.sleep(3)
        raise JobError("Time's up")


class MockedWorker2(MockedWorker):
    @schedulable(socket='dummy_port|REP')
    async def dummy_job(self, socket):
        await asyncio.sleep(0)

    @service(socket='dummy_port2|REP')
    async def dummy_job2(self, socket):
        await asyncio.sleep(0)


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
        self.assertEqual(str(cm.exception), 'Controller registration timeout')

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
            messages.append(socket.recv_string())
            socket.send_json({'dummy_port': starting_port + 1, 'dummy_port2': starting_port + 2})

            # mock heartbeat
            for _ in range(4):
                messages.append(socket.recv_string())
                socket.send_json({'code': const.CCode.Succeeded.value})

        t = threading.Thread(target=mock_controller)
        t.start()
        start = time.time()

        with self.assertRaises(JobError) as cm:
            asyncio.run(worker._run())
        self.assertEqual(str(cm.exception), 'Controller unreachable')

        duration = time.time() - start
        t.join()
        self.assertGreater(duration, 2)  # 4 heartbeats take 2s
        self.assertLess(duration, 2.5)  # wait time add 0.2, so the process should take around 2.2s
        self.assertListEqual(messages, ['test|dummy_port|dummy_port2', 'test', 'test', 'test', 'test'])
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


if __name__ == '__main__':
    unittest.main()
