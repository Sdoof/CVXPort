
import asyncio
import zmq.asyncio as azmq
from aiohttp import web
import pandas as pd
from typing import Dict

from cvxport.worker import Worker, service, schedulable
from cvxport import Config
from cvxport import const


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

        # add 0.5 to make sure all satellites have enough time to send heartbeat
        self.heartbeat_interval = Config['heartbeat_interval'] + 0.5
        self.current_usable_port = Config['starting_port']  # ports to be assigned to workers

        # ports
        self.port_map = {
            'http_port': Config['controller_http_port'],
            'controller_port': Config['controller_port'],
            'controller_comm_port': Config['controller_comm_port']
        }
        self.registry = {}
        self.data_servers = {}
        self.executors = {}

    # ==================== Registration ====================
    async def handle_registration(self, socket, req):
        name = req['name']  # type: str

        if 'ports' not in req:
            self.logger.warning(f'Missing ports in registration request {req}')
            await socket.send_json({'code': const.CCode.MissingRequiredPort.value})
            return

        ports = {p: -1 for p in req['ports']}  # type: Dict[str, int]

        # duplicated worker
        if name in self.registry:
            await socket.send_json({'code': const.CCode.AlreadyRegistered.value})
            return

        if name.startswith('DataServer:'):
            # noinspection PyBroadException
            try:
                broker_name = const.Broker(name.split(':')[1]).name  # implicitly check validity of broker
            except Exception:
                await socket.send_json({'code': const.CCode.UnKnownBroker.value})
                return

            # check ports
            if any(port not in ports
                   for port in ['subscription_port', 'data_port', 'order_port']):
                await socket.send_json({'code': const.CCode.MissingRequiredPort.value})
                return

            # check info
            if 'info' not in req or 'freq' not in req['info'] or 'offset' not in req['info']:
                await socket.send_json({'code': const.CCode.MissingDataServerInfo.value})
                return

            self.data_servers[broker_name] = {'ports': ports, 'info': req['info']}

        # assign ports
        for port in ports:
            ports[port] = self.current_usable_port
            self.current_usable_port += 1

        # register and return ports
        self.registry[name] = pd.Timestamp.now('EST')
        await socket.send_json(ports)
        self.logger.info(f'Registered "{name}"')

    # ==================== Heartbeat ====================
    @service(socket='controller_port|REP')
    async def handle_registration_and_heartbeat(self, socket: azmq.Socket):
        """
        1. handle registration
        2. update heartbeat
        """
        req = await socket.recv_json()

        # preliminary checks
        if 'type' not in req:
            self.logger.warning(f'Unknown request {req}')
            await socket.send_json({'code': const.CCode.UnknownRequest.value})
            return

        if 'name' not in req:
            self.logger.warning(f'Missing name {req}')
            await socket.send_json({'code': const.CCode.MissingName.value})
            return

        # registration
        if req['type'] == 'register':
            await self.handle_registration(socket, req)

        # heartbeat
        elif req['type'] == 'hb':
            name = req['name']
            if name in self.registry:
                self.registry[name] = pd.Timestamp.now('EST')
                await socket.send_json({'code': const.CCode.Succeeded.value})
            else:
                await socket.send_json({'code': const.CCode.NotInRegistry.value})
                self.logger.warning(f'Potentially lose track of registration {req}')

        else:
            await socket.send_json({'code': const.CCode.UnknownRequest.value})
            self.logger.warning(f'Receive improper registration request {req}')

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
        await web._run_app(app, host='localhost', port=self.port_map['http_port'])

    # ==================== Data Server ====================
    @service(socket='controller_comm_port|REP')
    async def handle_communication(self, socket: azmq.Socket):
        msg = await socket.recv_string()  # type: str
        self.logger.info(f'Receive Data Server request "{msg}"')
        if msg.startswith('DataServer:'):
            if msg in self.registry:
                await socket.send_json(self.data_servers[msg.split(':')[1]])
                self.logger.info('Sent Data Server ports and info')
            else:
                await socket.send_json({'code': const.CCode.ServerNotOnline.value})
                self.logger.info('Data Server is not registered yet')
        else:
            await socket.send_json({'code': const.CCode.UnknownRequest.value})
