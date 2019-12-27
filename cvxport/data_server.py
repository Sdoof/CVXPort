
import zmq.asyncio as azmq
from cvxport.worker import SatelliteWorker, service


class DataServer(SatelliteWorker):
    """
    Data server base class. Provides API template for
    1. reply subscribed symbols
    2.
    """
    def __init__(self, name):
        super(DataServer, self).__init__(name)
        self.subscription_port = 1234

    @service(socket='subscription_port|REP')
    async def process_symbol_subscription(self, in_socket: azmq.Socket, out_socket: azmq.Socket):
        while True:
            symbol = await in_socket.recv_string()  # type: str
            asset, ticker = symbol.split('|')

