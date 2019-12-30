
import abc
import zmq
import zmq.asyncio as azmq
import asyncio
import pystore as ps
from pystore.collection import Collection
from typing import List, Dict
import ib_insync as ibs
from datetime import datetime

from cvxport.worker import SatelliteWorker, service
from cvxport.data import Asset
from cvxport import utils, Config, JobError
from cvxport.const import Broker


# ==================== Helpers ====================
class Datum:
    def __init__(self, asset: Asset, date: datetime, open: float, high: float, low: float, close: float):
        self.asset = asset
        self.date = date
        self.open = open
        self.high = high
        self.low = low
        self.close = close

    def __str__(self):
        return f'{self.asset.string},{self.date.strftime( "%Y-%m-%d %H:%M:%S")},' \
               f'{self.open},{self.high},{self.low},{self.close}'


def convert_ib_bar_to_dict(asset: Asset, bar: ibs.BarData):
    # noinspection PyUnresolvedReferences
    return Datum(asset, bar.time, bar.open_, bar.high, bar.low, bar.close)


# ==================== Main Classes ====================
class DataServer(SatelliteWorker, abc.ABC):
    """
    Data server base class. Provides API template for
    1. reply subscribed symbols
    2.
    """
    def __init__(self, broker: Broker):
        super(DataServer, self).__init__(f'DataServer:{broker.name}')
        self.broker = broker
        self.data_queue = asyncio.Queue()
        self.ps_handles = {}  # type: Dict[Asset, Collection]

    # ==================== To override ====================
    @abc.abstractmethod
    async def subscribe(self, assets: List[Asset]):
        pass

    # ==================== Services ====================
    @service(socket='subscription_port|REP')
    async def process_subscription(self, socket: azmq.Socket):
        asset_strings = (await socket.recv_string()).split(',')  # format: asset1|ticker1,asset2|ticker2,...
        assets = [Asset(s) for s in asset_strings]

        # create pystore handles for asset
        ps.set_path(Config['pystore_path'])

        for asset in assets:
            store = ps.store(asset.asset)
            self.ps_handles[asset] = store.collection(asset.ticker)

        # submit subscription
        await utils.wait_for(self.subscribe(assets), Config['wait_time'], JobError('Data subscription timeout'))
        socket.send_string('0')

    @service(socket='broadcast_port|PUB')
    async def publish_and_save_data(self, socket: azmq.Socket):
        """
        This only deals with streaming data. Historical data is handled in another service
        """
        data = await self.data_queue.get()  # type: Datum
        await socket.send_string(str(data))  # send_pyobj() may cause incomplete packages
        self.ps_handles[data.asset].write(self.broker.name, data)


class IBDataServer(DataServer):
    def __init__(self):
        super(IBDataServer, self).__init__(Broker.IB)
        self.handles = {}
        self.ib = ibs.IB()
        self.ib.connect('127.0.0.1', Config['ib_port'])

    async def subscribe(self, assets: List[Asset]):
        def generate_callback(label_asset: Asset):
            def callback(bars, hasNewBar):
                if hasNewBar:
                    # nowait because it's used inside a regular lambda
                    self.data_queue.put_nowait(convert_ib_bar_to_dict(label_asset, bars[-1]))
            return callback

        for asset in assets:
            handle = self.ib.reqRealTimeBars(asset.to_ib_contract(), 5, 'MIDPOINT', False)
            handle.updateEvent += generate_callback(asset)
            self.handles[asset] = handle

    def shutdown(self):
        self.ib.disconnect()


def subscriber():
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect(Config[''])


if __name__ == '__main__':
    server = IBDataServer()
    server.run()