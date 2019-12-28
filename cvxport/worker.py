
import abc
import asyncio
import zmq
import zmq.asyncio as azmq
from typing import Awaitable, Callable
import inspect

from cvxport import utils
from cvxport import Config
from cvxport.basic_logging import Logger


# ==================== Decorators ====================
def generate_wrapper(priority: int, sockets):
    def wrapper(functor: Callable) -> Callable:
        functor.__job__ = priority
        functor.__sockets__ = sockets
        return functor
    return wrapper


def startup(**sockets):
    """
    To label the awaitable as start-up procedure that must succeed before services are run
    """
    return generate_wrapper(1, sockets)


def schedulable(**sockets):
    """
    To label the awaitable as job that runs after start-up jobs
    This provides a blank canvas to allow
    """
    # TODO: decide if we should add exception handling later
    return generate_wrapper(2, sockets)


def service(**sockets):
    """
    To label the awaitable as repetitive job with error handling
    """
    def wrapper(functor: Callable) -> Callable:
        async def loop_wrapper(self, **kwargs):
            while True:
                # noinspection PyBroadException
                try:
                    await functor(self, **kwargs)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    # This includes JobError.
                    # The awaitable should handle other exceptions. That's why we choose to break the loop here
                    raise e

        loop_wrapper.__job__ = 2
        loop_wrapper.__sockets__ = sockets
        return loop_wrapper
    return wrapper


# ==================== Exceptions ====================
class JobError(Exception):
    pass


# ==================== Main Classes ====================
class Worker(abc.ABC):
    """
    Base class for standalone worker, like executor, controller and data server
    "startup" are one-time jobs that must succeed before running services
    "service" are on-going jobs
    """
    # noinspection PyUnresolvedReferences
    protocol_map = {
        'PUSH': zmq.PUSH,
        'PULL': zmq.PULL,
        'PUB': zmq.PUB,
        'SUB': zmq.SUB,
        'REQ': zmq.REQ,
        'REP': zmq.REP,
    }

    def __init__(self, name):
        self.name = name
        self.logger = Logger(name)

    def run(self):
        # noinspection PyBroadException
        try:
            asyncio.run(self._run())
        except JobError as e:
            self.logger.warning(e)  # JobError is not unexpected errors.
        except Exception:
            self.logger.exception('Unexpected Error!')

    # ==================== Helper ====================
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

                if protocol == 'REQ':
                    # noinspection PyUnresolvedReferences
                    socket.setsockopt(zmq.LINGER, 0)

                # default bind / connect classifications. May need extension in the future?
                if protocol in ['PUSH', 'SUB', 'REQ']:
                    socket.connect(address)
                    print(f'{spec}: connect {address}')
                elif protocol in ['PULL', 'PUB', 'REP']:
                    socket.bind(address)
                    print(f'{spec}: bind {address}')
                else:
                    raise ValueError(f'Protocol {protocol} is not in any bind/connect category')

                sockets[spec] = socket

            else:
                raise ValueError(f'Port {port_name} not defined')

        # ---------- Start jobs ----------
        job_groups = {}
        for job in job_list:
            job_groups.setdefault(job.__job__, []).append(job)  # group jobs by priority

        try:
            groups = sorted(job_groups.keys())
            for group in groups:
                jobs_per_group = job_groups[group]
                awaitables = []
                for job in jobs_per_group:
                    inputs = {k: sockets[v] for k, v in job.__sockets__.items()}
                    awaitables.append(job(**inputs))
                await asyncio.gather(*awaitables)
        finally:
            for socket in sockets.values():
                socket.close()


class SatelliteWorker(Worker):
    """
    1. register with controller
    2. keep track of connection with controller
    """
    def __init__(self, name: str, num_ports: int):
        super(SatelliteWorker, self).__init__(name)
        self.num_ports = num_ports
        self.wait_time = Config['startup_wait_time']

        # make sure heartbeat is sent at least once between registry check
        self.heartbeat_interval = Config['heartbeat_interval'] - 1

        # set up ports
        self.controller_port = Config['controller_port']
        self.starting_port = None

    # ==================== Startup ====================
    @startup(socket='controller_port|REQ')
    async def register(self, socket: azmq.Socket):
        await socket.send_string(f'{self.name}|{self.num_ports}')
        port = await utils.wait_for(socket.recv_string(), self.wait_time, JobError('Controller registration timeout'))
        self.starting_port = int(port)
        if self.starting_port < 0:
            raise JobError(f'{self.name} already registered')

    # ==================== Services ====================
    @service(socket='controller_port|REQ')
    async def emit_heartbeat(self, socket: azmq.Socket):
        await socket.send_string(self.name)
        ind = int(await utils.wait_for(socket.recv_string(), self.wait_time, JobError('Controller unreachable')))
        if ind < 0:
            raise JobError('Controller registration lost')
        await asyncio.sleep(self.heartbeat_interval)
