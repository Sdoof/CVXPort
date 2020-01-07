
import abc
import asyncio
import zmq
import zmq.asyncio as azmq
from typing import Callable
import inspect

from cvxport import utils, Config, const, JobError
from cvxport.basic_logging import Logger


# ==================== Decorators ====================
def generate_wrapper(priority: int, sockets):
    def wrapper(functor: Callable) -> Callable:
        functor.__job__ = priority
        functor.__sockets__ = sockets
        return functor
    return wrapper


def startup(*priority, **sockets):
    """
    To label the awaitable as start-up procedure that must succeed before services are run
    """
    rank = 1
    if len(priority) == 1:
        priority = priority[0]
        if not isinstance(priority, int):
            raise ValueError('Priority should be integer')

        if priority < 0 or priority > 9:
            raise ValueError('Priority should be within 0 to 9')

        rank += priority / 10

    elif len(priority) > 1:
        raise ValueError('Too many arguments')

    return generate_wrapper(rank, sockets)


def schedulable(**sockets):
    """
    To label the awaitable as job that runs after start-up jobs
    This provides a blank canvas to allow
    """
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

    stage_map = {
        1: 'Execute start-up sequences',
        2: 'Online'
    }

    def __init__(self, name: str):
        if '|' in name:
            raise ValueError("Worker name can't contain '|'")
        self.name = name
        self.logger = Logger(name)
        self.port_map = {}
        self.logger.info(f'Starting "{name}" ...')

    async def shutdown(self):
        pass

    def run(self):
        # noinspection PyBroadException
        try:
            asyncio.run(self._run())
        except JobError as e:
            self.logger.warning(e)  # JobError is not unexpected errors.
            self.logger.info('Exited')
        except Exception:
            self.logger.info('Exited')
            self.logger.exception('Unexpected Error!')

    # ==================== Helper ====================
    async def _run(self):
        """
        asyncio context and sockets have to be set up within a coroutine. Otherwise, they won't function as expected
        Also, if we declare asyncio context outside a coroutine, the program won't exit smoothly
        """
        # ---------- Retrieve all schedulable jobs and run according to priorities ----------
        all_jobs = [job for _, job in inspect.getmembers(self, inspect.ismethod) if getattr(job, '__job__', False)]
        job_groups = {}
        for job in all_jobs:
            job_groups.setdefault(job.__job__, []).append(job)  # group jobs by priority

        ordered_groups = sorted(job_groups.keys())
        for group in ordered_groups:
            self.logger.info('')
            self.logger.info(f'========== {Worker.stage_map[int(group)]} ==========')  # log stage
            jobs_per_group = job_groups[group]

            # get all socket specs
            socket_list = utils.unique(utils.flatten(list(job.__sockets__.values()) for job in jobs_per_group))

            # check if specs are unique
            minimal_sockets = utils.unique(spec.split('|')[0] for spec in socket_list)
            if len(minimal_sockets) < len(socket_list):
                raise ValueError(f'Duplicated socket specification: {socket_list}')

            # ---------- Initialize sockets ----------
            context = azmq.Context()
            sockets = {}

            for spec in socket_list:
                port_name, protocol = spec.split('|')
                port = self.port_map.get(port_name, None)

                if port == -1:
                    sockets[spec] = None

                elif port is not None:
                    socket = context.socket(Worker.protocol_map[protocol])
                    address = f'tcp://127.0.0.1:{port}'

                    # this is required for using timeout on REQ. Otherwise, socket blocks forever
                    if protocol == 'REQ':
                        # noinspection PyUnresolvedReferences
                        socket.setsockopt(zmq.LINGER, 0)

                    # default bind / connect classifications. May need extension in the future?
                    if protocol in ['PUSH', 'SUB', 'REQ']:
                        socket.connect(address)
                        self.logger.info(f'{spec}: contacting {address}')
                    elif protocol in ['PULL', 'PUB', 'REP']:
                        socket.bind(address)
                        self.logger.info(f'{spec}: listening on {address}')
                    else:
                        raise ValueError(f'Protocol {protocol} is not in any bind/connect category')

                    sockets[spec] = socket

                else:
                    raise ValueError(f'Port {port_name} not defined')

            # ---------- Start jobs ----------
            try:
                awaitables = []
                for job in jobs_per_group:
                    inputs = {k: sockets[v] for k, v in job.__sockets__.items()}
                    awaitables.append(job(**inputs))
                await asyncio.gather(*awaitables)
            finally:
                for socket in sockets.values():
                    socket.close()

                if group == 2:
                    self.logger.info('\n========== Shutting Down ==========')
                    await self.shutdown()


class SatelliteWorker(Worker):
    """
    1. register with controller
    2. keep track of connection with controller
    """
    def __init__(self, name: str, registration_info: dict = None):
        """
        :param name: worker name
        :param registration_info: extra information to be sent in registration
        """
        super(SatelliteWorker, self).__init__(name)
        self.wait_time = Config['startup_wait_time']
        self.registration_info = registration_info

        # make sure heartbeat is sent at least once between registry check
        self.heartbeat_interval = Config['heartbeat_interval'] - 1

        # set up ports
        self.port_map['controller_port'] = Config['controller_port']

    # ==================== Startup ====================
    @startup(5, socket='controller_port|REQ')  # Use priority 5 so that we can insert job before after this if needed
    async def register(self, socket: azmq.Socket):
        # figure out ports to request for
        jobs = [job for _, job in inspect.getmembers(self, inspect.ismethod) if getattr(job, '__job__', 0) > 1]
        names = [s.split('|')[0] for s in utils.flatten(list(job.__sockets__.values()) for job in jobs)]
        minimal_names = sorted([name for name in utils.unique(names) if name not in self.port_map])

        # request for ports
        msg = {'name': self.name, 'type': 'register', 'ports': minimal_names}
        if self.registration_info:
            msg['info'] = self.registration_info
        await socket.send_json(msg)

        ports = await utils.wait_for_reply(socket, self.wait_time, const.CCode, 'Controller registration')
        self.port_map.update(ports)
        self.logger.info(f'Successfully registered {self.name}')

    # ==================== Services ====================
    @service(socket='controller_port|REQ')
    async def emit_heartbeat(self, socket: azmq.Socket):
        await socket.send_json({'name': self.name, 'type': 'hb'})
        await utils.wait_for_reply(socket, self.wait_time, const.CCode, 'Heartbeat')
        await asyncio.sleep(self.heartbeat_interval)
