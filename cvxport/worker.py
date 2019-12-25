
import abc
import asyncio
import zmq
import zmq.asyncio as azmq
from typing import Union, Awaitable
import inspect

from cvxport import utils


def service(**sockets):
    def wrapper(functor: Awaitable) -> Awaitable:
        functor.__job__ = True
        functor.__sockets__ = sockets
        return functor
    return wrapper


class Worker(abc.ABC):
    """
    Base class for standalone worker, like executor, controller and data server
    provides
    1. heartbeat
    2. "job" decorator to indicate a method as async job
    """
    protocol_map = {
        'PUSH': zmq.PUSH,
        'PULL': zmq.PULL,
        'PUB': zmq.PUB,
        'SUB': zmq.SUB
    }

    def __init__(self, name, heartbeat_port, heartbeat_interval=60):
        self.name = name
        self.job_list = []
        self.dummy_port = self.heartbeat_port = heartbeat_port
        self.heartbeat_interval = heartbeat_interval

    def run(self):
        asyncio.run(self._run())

    # ==================== Services ====================
    @service(out_socket='heartbeat_port|PUSH')
    async def emit_heartbeat(self, out_socket: Union[azmq.Socket, None]):
        while True:
            await out_socket.send_string(self.name)
            await asyncio.sleep(self.heartbeat_interval)

    # ==================== Private ====================
    async def _run(self):
        """
        asyncio context and sockets have to be set up within a coroutine. Otherwise, they won't function as expected
        Also, if we declare asyncio context outside a coroutine, the program won't exit smoothly
        """
        # ---------- Retrieve and check jobs (services) ----------
        job_list = [job for _, job in inspect.getmembers(self, inspect.ismethod) if getattr(job, '__job__', False)]
        socket_list = utils.unique(utils.flatten(list(job.__sockets__.values()) for job in job_list))

        # check socket specification
        minimal_sockets = utils.unique(spec.split('|')[0] for spec in socket_list)
        if len(minimal_sockets) < len(socket_list):
            raise ValueError(f'Duplicated socket specification: {socket_list}')

        # ---------- Initialize sockets ----------
        context = azmq.Context()
        sockets = {}

        for spec in socket_list:
            port_name, protocol = spec.split('|')
            port = getattr(self, port_name, -1)
            if port > 0:
                socket = context.socket(Worker.protocol_map[protocol])
                address = f'tcp://127.0.0.1:{port}'

                # default bind / connect classifications. May need extension in the future?
                if protocol in ['PUSH', 'SUB']:
                    socket.connect(address)
                    print(f'{spec}: connect {address}')
                elif protocol in ['PULL', 'PUB']:
                    socket.bind(address)
                    print(f'{spec}: bind {address}')
                else:
                    raise ValueError(f'Protocol {protocol} is not in any bind/connect category')

                sockets[spec] = socket

            else:
                raise ValueError(f'Port {port_name} not defined')

        # ---------- Start jobs ----------
        jobs = []
        for job in job_list:
            inputs = {k: sockets[v] for k, v in job.__sockets__.items()}
            jobs.append(job(**inputs))
        await asyncio.gather(*jobs)


if __name__ == '__main__':
    worker = Worker('abc', 12345)
    worker.run()
