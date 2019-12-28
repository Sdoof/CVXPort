
import asyncio
import zmq.asyncio as azmq
from aiohttp import web
import pandas as pd

from cvxport.worker import Worker, service, schedulable
from cvxport import Config


class Controller(Worker):
    """
    Controller class to provide
    1. web base control panel
    2. heartbeat registry - track heartbeats of data servers and executors
    3. start new data servers per request from executors
    4. email / msg notification
    """
    def __init__(self, name='Controller'):
        super(Controller, self).__init__(name)
        self.controller_port = Config['controller_port']
        # add 0.5 to make sure all satellites have enough time to send heartbeat
        self.heartbeat_interval = Config['heartbeat_interval'] + 0.5
        self.http_port = Config['controller_http_port']
        self.registry = {}
        self.current_usable_port = Config['starting_port']  # ports to be assigned to workers

        # print out
        self.logger.info('Controller online')

    # ==================== Heartbeat ====================
    @service(socket='controller_port|REP')
    async def handle_registration_and_heartbeat(self, socket: azmq.Socket):
        """
        1. handle registration
        2. update heartbeat
        """
        raw = await socket.recv_string()
        msg = raw.split('|')  # either "name|num_ports" or "name"

        # first time registration
        if len(msg) == 2:
            name, num_ports = msg[0], int(msg[1])

            # duplicated worker
            if name in self.registry:
                await socket.send_string('-1')
            else:
                # register and return starting port
                self.registry[name] = pd.Timestamp.now('EST')
                await socket.send_string(f'{self.current_usable_port}')
                self.current_usable_port += num_ports

        # heartbeat
        elif len(msg) == 1:
            name = msg[0]
            if name in self.registry:
                self.registry[name] = pd.Timestamp.now('EST')
                await socket.send_string('0')
            else:
                await socket.send_string('-1')
                self.logger.warning(f'Potentially lose track of registration {raw}')

        else:
            await socket.send_string('-1')  # doesn't conform to any format
            self.logger.warning(f'Receive improper registration request {raw}')

    @service()
    async def organize_registry(self):
        """
        Remove obsolete heartbeat
        """
        now = pd.Timestamp.now('EST')
        to_remove = []
        for name, last_update in self.registry.items():
            if (now - last_update).total_seconds() > self.heartbeat_interval:
                to_remove.append(name)
                self.logger.warning(f'{name} connection lost')

        for name in to_remove:
            del self.registry[name]

        await asyncio.sleep(self.heartbeat_interval)

    # ==================== Http ====================
    @schedulable()  # loop is handled in web._run_app
    async def show_status(self):
        async def show(request):
            text = str(self.registry)
            return web.Response(text=text)

        app = web.Application()
        app.add_routes([web.get('/', show)])

        # noinspection PyProtectedMember
        await web._run_app(app, host='localhost', port=self.http_port)

    # ==================== Data Server ====================
    # @service(in_socket='data_port|PULL')
    # async def start_data_server(self):
    #     """
    #     1. Start data server (per broker) if needed
    #     2. Add symbol to running data servers
    #     3. Return to executors ports from which data can be retrieved
    #     """
    #     while True:
    #         pass
