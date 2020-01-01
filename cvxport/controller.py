
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

    # ==================== Heartbeat ====================
    @service(socket='controller_port|REP')
    async def handle_registration_and_heartbeat(self, socket: azmq.Socket):
        """
        1. handle registration
        2. update heartbeat
        # TODO: should implement port recycling in the future
        """
        raw = await socket.recv_string()
        msg = raw.split('|')  # either "name|port1|port2..." or "name"

        # registration
        if len(msg) > 1:
            name = msg[0]  # type: str
            ports = {p: 0 for p in msg[1:] if p != ''}  # type: Dict[str, int]

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

                if 'subscription_port' not in ports or 'broadcast_port' not in ports:
                    await socket.send_json({'code': const.CCode.MissingRequiredPort.value})
                    return

                self.data_servers[broker_name] = ports
                self.logger.info(f'Registering data server {broker_name}')

            # assign ports
            for port in ports:
                ports[port] = self.current_usable_port
                self.current_usable_port += 1

            # register and return ports
            self.registry[name] = pd.Timestamp.now('EST')
            await socket.send_json(ports)

        # heartbeat
        elif len(msg) == 1:
            name = msg[0]
            if name in self.registry:
                self.registry[name] = pd.Timestamp.now('EST')
                await socket.send_json({'code': const.CCode.Succeeded.value})
            else:
                await socket.send_json({'code': const.CCode.NotInRegistry.value})
                self.logger.warning(f'Potentially lose track of registration {raw}')

        else:
            await socket.send_json({'code': const.CCode.UnknownRequest.value})
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
        await web._run_app(app, host='localhost', port=self.port_map['http_port'])

    # ==================== Data Server ====================
    @service(socket='controller_comm_port|REP')
    async def handle_communication(self, socket: azmq.Socket):
        msg = await socket.recv_string()  # type: str
        if msg.startswith('DataServer:'):
            if msg in self.registry:
                ports = self.data_servers[msg.split(':')[1]]
                await socket.send_json({p: n for p, n in ports.items() if p in ['subscription_port', 'broadcast_port']})
            else:
                await socket.send_json({'code': const.CCode.ServerNotOnline.value})
        else:
            await socket.send_json({'code': const.CCode.UnknownRequest.value})
