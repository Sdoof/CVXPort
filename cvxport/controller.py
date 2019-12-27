
import asyncio
import zmq.asyncio as azmq
from aiohttp import web
import pandas as pd

from cvxport.worker import Worker, service
from cvxport import Config


class Controller(Worker):
    """
    Controller class to provide
    1. web base control panel
    2. heartbeat registry - track heartbeats of data servers and executors
    3. start new data servers per request from executors
    4. email / msg notification
    """
    def __init__(self):
        super(Controller, self).__init__('Controller')
        self.heartbeat_port = Config['heartbeat_port']
        self.heartbeat_interval = Config['heartbeat_interval']
        self.http_port = Config['http_port']
        self.registry = {}

    # ==================== Heartbeat ====================
    @service(in_socket='heartbeat_port|PULL')
    async def register_heartbeat(self, in_socket: azmq.Socket):
        """
        Receiving heartbeats
        """
        while True:
            name = await in_socket.recv_string()
            self.registry[name] = pd.Timestamp.now('EST')

    @service()
    async def track_heartbeat_registry(self):
        """
        Remove obsolete heartbeat and log
        """
        while True:
            now = pd.Timestamp.now('EST')
            to_remove = []
            for name, last_update in self.registry.items():
                if (now - last_update).seconds > self.heartbeat_interval:
                    to_remove.append(name)
                    print(f'{name} connection lost')

            for name in to_remove:
                del self.registry[name]

            await asyncio.sleep(self.heartbeat_interval)

    # ==================== Http ====================
    @service()
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

    @service()
    async def test_heartbeat(self):
        import zmq
        context = azmq.Context()
        socket = context.socket(zmq.PUSH)
        socket.connect(f'tcp://127.0.0.1:{self.heartbeat_port}')
        for _ in range(5):
            socket.send_string('test Bot')
            await asyncio.sleep(5)


if __name__ == '__main__':
    controller = Controller()
    controller.run()