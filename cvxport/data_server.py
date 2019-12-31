
import abc
import zmq.asyncio as azmq
import asyncio
import pystore as ps
from pystore.collection import Collection
from typing import List, Dict
import ib_insync as ibs

from cvxport.worker import SatelliteWorker, service, startup
from cvxport.data import Asset, Datum
from cvxport import utils, Config, JobError, const


# ==================== Helpers ====================
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
    def __init__(self, broker: const.Broker, save_data=False):
        super(DataServer, self).__init__(f'DataServer:{broker.name}')
        self.broker = broker
        self.save_data = save_data
        self.subscription_wait_time = Config['subscription_wait_time']
        self.data_queue = None
        self.subscribed = {}
        self.ps_handles = {}  # type: Dict[Asset, Collection]

    # ==================== To override ====================
    @abc.abstractmethod
    async def subscribe(self, assets: List[Asset]):
        pass

    @startup()
    async def startup(self):
        self.data_queue = asyncio.Queue()

    # ==================== Services ====================
    @service(socket='subscription_port|REP')
    async def process_subscription(self, socket: azmq.Socket):
        asset_strings = (await socket.recv_string()).split(',')  # format: asset1|ticker1,asset2|ticker2,...
        assets = [Asset(s) for s in asset_strings]

        # create pystore handles for asset
        ps.set_path(Config['pystore_path'])

        if self.save_data:
            for asset in assets:
                store = ps.store(asset.asset)
                self.ps_handles[asset] = store.collection(asset.ticker)

        # submit subscription
        await utils.wait_for(self.subscribe(assets), self.subscription_wait_time, JobError('Data subscription timeout'))
        self.subscribed.update(dict.fromkeys(assets))  # add subscription
        socket.send_string(str({'code': const.DCode.Succeeded.value}))

    @service(socket='broadcast_port|PUB')
    async def publish_and_save_data(self, socket: azmq.Socket):
        """
        This only deals with streaming data. Historical data is handled in another service
        """
        data = await self.data_queue.get()  # type: Datum
        await socket.send_string(str(data))  # send_pyobj() may cause incomplete packages

        if self.save_data:
            self.ps_handles[data.asset].write(self.broker.name, data)


class IBDataServer(DataServer):
    def __init__(self):
        super(IBDataServer, self).__init__(const.Broker.IB, save_data=True)
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
