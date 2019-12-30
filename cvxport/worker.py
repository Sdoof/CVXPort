
import abc
import asyncio
import zmq
import zmq.asyncio as azmq
from typing import Callable
import inspect

from cvxport import utils, Config, const
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

    stage_map = {
        1: 'Execute start-up sequences',
        2: 'Worker comes online'
    }

    def __init__(self, name: str):
        if not name[0].isalpha() or not name.isalnum():
            raise ValueError('Worker name has to start with alphabet and contain only alphanumeric')
        self.name = name
        self.logger = Logger(name)
        self.port_map = {}
        self.logger.info(f'Starting "{name}" ...')

    def shutdown(self):
        pass

    def run(self):
        # noinspection PyBroadException
        try:
            self._run_worker(self._run())
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
        # ---------- Retrieve all schedulable jobs and run according to priorities ----------
        all_jobs = [job for _, job in inspect.getmembers(self, inspect.ismethod) if getattr(job, '__job__', False)]
        job_groups = {}
        for job in all_jobs:
            job_groups.setdefault(job.__job__, []).append(job)  # group jobs by priority

        ordered_groups = sorted(job_groups.keys())
        for group in ordered_groups:
            self.logger.info('')
            self.logger.info(f'========== {Worker.stage_map[group]} ==========')  # log stage
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
                if port is not None:
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

    def _run_worker(self, awaitables):
        """
        To mimic asyncio.run but use get_event_loop in place of new_event_loop
        This is needed in the case of ib_insync
        """

        # noinspection PyProtectedMember
        # W
        if asyncio.events._get_running_loop() is not None:
            raise RuntimeError("asyncio.run() cannot be called from a running event loop")

        self.loop = asyncio.get_event_loop()
        try:
            return self.loop.run_until_complete(awaitables)
        finally:
            try:
                self._cancel_all_tasks()
                self.loop.run_until_complete(self.loop.shutdown_asyncgens())
            finally:
                # we don't close the loop here because we are using asyncio.get_event_loop now
                # subsequent async code can't be run if we close it
                # In practise, we are only running one worker. This choice shouldn't be a problem
                self.shutdown()
                self.logger.warning('loop is closed')

    def _cancel_all_tasks(self):
        """
        Mimic for asyncio.run but rewrite with public methods
        """
        to_cancel = asyncio.all_tasks(self.loop)
        if not to_cancel:
            return

        for task in to_cancel:
            task.cancel()

        self.loop.run_until_complete(asyncio.gather(*to_cancel, loop=self.loop, return_exceptions=True))

        for task in to_cancel:
            if task.cancelled():
                continue
            if task.exception() is not None:
                self.loop.call_exception_handler({
                    'message': 'unhandled exception during asyncio.run() shutdown',
                    'exception': task.exception(),
                    'task': task,
                })


class SatelliteWorker(Worker):
    """
    1. register with controller
    2. keep track of connection with controller
    """
    def __init__(self, name: str):
        super(SatelliteWorker, self).__init__(name)
        self.wait_time = Config['startup_wait_time']

        # make sure heartbeat is sent at least once between registry check
        self.heartbeat_interval = Config['heartbeat_interval'] - 1

        # set up ports
        self.port_map['controller_port'] = Config['controller_port']

    # ==================== Startup ====================
    @startup(socket='controller_port|REQ')
    async def register(self, socket: azmq.Socket):
        # figure out number of ports need to be requested
        jobs = [job for _, job in inspect.getmembers(self, inspect.ismethod) if getattr(job, '__job__', 0) > 1]
        names = [s.split('|')[0] for s in utils.flatten(list(job.__sockets__.values()) for job in jobs)]
        minimal_names = sorted([name for name in utils.unique(names) if name not in self.port_map])

        # request for ports
        await socket.send_string(f'{self.name}|{"|".join(minimal_names)}')
        msg = await utils.wait_for(socket.recv_string(), self.wait_time, JobError('Controller registration timeout'))
        ports = eval(msg)  # type: dict
        if ports.get('err', 0) < 0:
            raise JobError(const.ErrorCode(ports['err']).name)

        self.port_map.update(ports)

    # ==================== Services ====================
    @service(socket='controller_port|REQ')
    async def emit_heartbeat(self, socket: azmq.Socket):
        await socket.send_string(self.name)
        ind = eval(await utils.wait_for(socket.recv_string(), self.wait_time, JobError('Controller unreachable')))
        if ind.get('err', 0) < 0:
            raise JobError(const.ErrorCode(ind['err']).name)
        await asyncio.sleep(self.heartbeat_interval)
