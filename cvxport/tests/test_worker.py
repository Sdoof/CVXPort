
import asyncio
import unittest
import time
import threading
import zmq

from cvxport.worker import SatelliteWorker, service, WorkerException


# mock class
class MockWorker(SatelliteWorker):
    @service()
    async def shutdown(self):
        # await asyncio.sleep(3)
        print('yes here!')
        raise WorkerException('Timesup')


class TestSatelliteWorker(unittest.TestCase):
    def test_registration_timeout(self):
        """
        Test if controller registration will time out as expected
        """
        worker = MockWorker('test', 5)
        worker.wait_time = 1000  # 1 second

        start = time.time()

        with self.assertRaises(WorkerException) as cm:
            asyncio.run(worker._run())
        self.assertEqual(str(cm.exception), 'Registration with controller timeout')

        duration = time.time() - start
        # since we set the wait time to be 1 second, the worker should return between 1 to 1.2 seconds
        self.assertGreater(duration, 1)
        self.assertLess(duration, 1.2)

    def test_registration_rejection(self):
        """
        Test if controller registration is rejected
        """
        worker = MockWorker('test', 5)

        def mock_controller():
            context = zmq.Context()
            # noinspection PyUnresolvedReferences
            socket = context.socket(zmq.REP)
            socket.bind(f'tcp://127.0.0.1:{worker.controller_port}')
            socket.recv_string()
            socket.send_string('-1')

        # start controller
        t = threading.Thread(target=mock_controller)
        t.start()

        start = time.time()

        with self.assertRaises(WorkerException) as cm:
            asyncio.run(worker._run())
        self.assertEqual(str(cm.exception), 'test already registered')

        duration = time.time() - start
        t.join()
        # This test should return almost immediately
        self.assertLess(duration, 0.5)

    def test_registration_and_heartbeat(self):
        worker = MockWorker('test', 5)
        worker.heartbeat_interval = 0.5
        messages = []

        def mock_controller():
            context = zmq.Context()
            # noinspection PyUnresolvedReferences
            socket = context.socket(zmq.REP)
            socket.bind(f'tcp://127.0.0.1:{worker.controller_port}')
            for _ in range(5):
                messages.append(socket.recv_string())
                socket.send_string(f'{worker.controller_port + 1}')

        t = threading.Thread(target=mock_controller)
        t.start()
        start = time.time()

        # noinspection PyBroadException
        with self.assertRaises(WorkerException) as cm:
            asyncio.run(worker._run())
        print(messages)
        self.assertEqual(str(cm.exception), 'Controller unreachable')

        duration = time.time() - start
        t.join()

        self.assertGreater(duration, 2)
        self.assertLess(duration, 2.5)


if __name__ == '__main__':
    unittest.main()