
import asyncio
import unittest
import time
import threading
import zmq

from cvxport.worker import SatelliteWorker, service, schedulable, JobError


# mock class
class MockWorker(SatelliteWorker):
    @schedulable()
    async def shutdown(self):
        await asyncio.sleep(3)
        raise JobError("Time's up")


class TestSatelliteWorker(unittest.TestCase):
    def test_registration_timeout(self):
        """
        Test if controller registration will time out as expected
        """
        worker = MockWorker('test')
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
        worker = MockWorker('test')

        def mock_controller():
            context = zmq.Context()
            # noinspection PyUnresolvedReferences
            socket = context.socket(zmq.REP)
            socket.bind(f'tcp://127.0.0.1:{worker.port_map["controller_port"]}')
            socket.recv_string()
            socket.send_string('-1')

        # start controller
        t = threading.Thread(target=mock_controller)
        t.start()

        start = time.time()

        with self.assertRaises(JobError) as cm:
            asyncio.run(worker._run())
        self.assertEqual(str(cm.exception), 'test already registered')

        duration = time.time() - start
        t.join()
        # This test should return almost immediately
        self.assertLess(duration, 0.5)

    def test_registration_and_heartbeat(self):
        worker = MockWorker('test')
        worker.heartbeat_interval = 0.5
        worker.wait_time = 0.2

        messages = []

        def mock_controller():
            context = zmq.Context()
            # noinspection PyUnresolvedReferences
            socket = context.socket(zmq.REP)
            socket.bind(f'tcp://127.0.0.1:{worker.port_map["controller_port"]}')
            for _ in range(5):
                messages.append(socket.recv_string())
                socket.send_string(f'{worker.port_map["controller_port"] + 1}')

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
        self.assertListEqual(messages, ['test|0', 'test', 'test', 'test', 'test'])

    def test_registration_lost(self):
        worker = MockWorker('test')
        worker.heartbeat_interval = 0.5
        worker.wait_time = 0.2

        def mock_controller():
            context = zmq.Context()
            # noinspection PyUnresolvedReferences
            socket = context.socket(zmq.REP)
            socket.bind(f'tcp://127.0.0.1:{worker.port_map["controller_port"]}')
            socket.recv_string()
            socket.send_string(f'{1234}')  # fake port
            socket.recv_string()
            socket.send_string('-1')  # mock registration lost

        t = threading.Thread(target=mock_controller)
        t.start()

        with self.assertRaises(JobError) as cm:
            asyncio.run(worker._run())
        self.assertEqual(str(cm.exception), 'Controller registration lost')

        t.join()


if __name__ == '__main__':
    unittest.main()
