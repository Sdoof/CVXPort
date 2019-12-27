import threading
import asyncio
import zmq
from cvxport.worker import SatelliteWorker, service, JobError


class MockWorker(SatelliteWorker):
    @service()
    async def shutdown(self):
        await asyncio.sleep(3)
        raise JobError('Timesup')


worker = MockWorker('test', 5)
worker.heartbeat_interval = 0.5
worker.wait_time = 0.1

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
try:
    asyncio.run(worker._run())
except Exception as e:
    print(str(e))

t.join()

