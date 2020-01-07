
import abc
import zmq.asyncio as azmq
import asyncio
from typing import List, Dict
import ib_insync as ibs

from cvxport.worker import SatelliteWorker, service, startup
from cvxport.data import Asset, Datum, DataStore
from cvxport import utils, Config, JobError, const


# ==================== Helpers ====================
def convert_ib_bar_to_dict(asset: Asset, bar: ibs.BarData):
    # noinspection PyUnresolvedReferences
    return Datum(asset, bar.time, bar.open_, bar.high, bar.low, bar.close)


# ==================== Base Class ====================
class DataServer(SatelliteWorker, abc.ABC):
    """
    Data server base class. Provides API template for
    1. reply subscribed symbols
    2.
    """
    def __init__(self, broker: const.Broker, update_freq: const.Freq, offset: 0):
        super(DataServer, self).__init__(f'DataServer:{broker.name}', {'freq': update_freq.value, 'offset': offset})
        self.broker = broker
        self.update_freq = update_freq
        self.offset = offset

        self.subscription_wait_time = Config['subscription_wait_time']
        self.data_queue = None
        self.subscribed = {}
        self.store = DataStore(broker, update_freq)  # we use 5sec bar from IB

    @startup()
    async def _startup(self):
        self.data_queue = asyncio.Queue()
        await self.store.connect()

    # -------------------- To override --------------------
    @abc.abstractmethod
    async def subscribe(self, assets: List[Asset]):
        """
        Handle subscription to broker
        """
        pass

    @abc.abstractmethod
    async def execute(self, name: str, order: Dict[Asset, int]) -> dict:
        """
        executions is of the form {'asset1': [shares, price, commission], ...}
        """
        pass

    async def clean_up(self):
        pass

    # -------------------- Services --------------------
    @service(socket='subscription_port|REP')
    async def process_subscription(self, socket: azmq.Socket):
        """
        Receive request of the form "asset1:ticker1,asset2:ticker2,..."
        Subscribe to the broker
        TODO: add handling for exception. Currently always succeed
        """
        msg = await socket.recv_string()  # format: asset1:ticker1,asset2:ticker2,...
        self.logger.info(f'Receive subscription request on {msg}')

        # filter existing assets
        asset_strings = msg.split(',')
        assets = [Asset(s) for s in asset_strings]
        assets = [asset for asset in assets if asset not in self.subscribed]

        if len(assets) > 0:
            # submit subscription
            await utils.wait_for(self.subscribe(assets),
                                 self.subscription_wait_time,
                                 JobError('Data subscription timeout'))

            # remember newly added subscription
            self.subscribed.update(dict.fromkeys(assets))
            self.logger.info(f'Subscribed: {str(assets)}')

        else:
            self.logger.info(f'No new subscription is needed')

        socket.send_json({'code': const.DCode.Succeeded.value})

    @service(socket='data_port|PUB')
    async def publish_and_save_data(self, socket: azmq.Socket):
        """
        This only deals with streaming data. Historical data is handled in another service
        """
        data = await self.data_queue.get()  # type: Datum
        await socket.send_string(str(data))  # use send py object may cause incomplete packages
        await self.store.append(data)

    # -------------------- Order handling --------------------
    @service(socket='order_port|REP')
    async def execute_order(self, socket: azmq.Socket):
        """
        TODO: spawn out task for each request so that we can attend to other requests while waiting for fills
        We need an extra port to do this. Executor also need to provide a port
        For, we handle order sequentially
        """
        struct = await socket.recv_json()  # type: dict
        self.logger.info(f'Receive order: {struct}')

        order = {Asset(k): v for k, v in struct.items() if k != 'Strategy'}
        executions = await self.execute(struct['Strategy'], order)

        self.logger.info(f'Executed order for {struct["Strategy"]} {str(executions)}')

        await socket.send_json(executions)

    async def shutdown(self):
        await self.clean_up()
        await self.store.disconnect()
        self.logger.info('Exited data server')


# ==================== Interactive Brokers ====================
class IBDataServer(DataServer):
    quote_type_map = {
        const.AssetClass.FX: 'MIDPOINT',
        const.AssetClass.STK: 'TRADES',
    }  # type: Dict[const.AssetClass, str]

    def __init__(self):
        super(IBDataServer, self).__init__(const.Broker.IB, const.Freq.SECOND5, offset=1)
        self.handles = {}
        # noinspection PyTypeChecker
        self.ib = None  # type: ibs.IB

    @startup()
    async def initialize_ib_connection(self):
        self.ib = ibs.IB()

        try:
            await self.ib.connectAsync('127.0.0.1', Config['ib_port'])
        except asyncio.TimeoutError:
            raise JobError('IB Gateway is not online')

        self.logger.info(f'IB connected')

    # -------------------- Helpers --------------------
    @staticmethod
    def contract_to_string(contract):
        if isinstance(contract, ibs.Forex):
            return f'FX:{contract.symbol}{contract.currency}'
        elif isinstance(contract, ibs.Stock):
            return f'STK:{contract.symbol}'

    # -------------------- IB Connection --------------------
    @service()
    async def check_gateway_connection(self):
        if not self.ib.isConnected():
            self.logger.warning('Gateway connection lost. Trying to reconnect ...')

            duration = self.heartbeat_interval
            while not self.ib.isConnected():
                # noinspection PyBroadException
                try:
                    await self.ib.connectAsync('127.0.0.1', Config['ib_port'])
                except Exception:
                    self.logger.info(f'Retry connecting in {duration}s')
                    await asyncio.sleep(duration)

            self.logger.info('Gateway connection re-established')

        await asyncio.sleep(self.heartbeat_interval)

    # -------------------- Subscription --------------------
    async def subscribe(self, assets: List[Asset]):
        def generate_callback(label_asset: Asset):
            def callback(bars, has_new_bar):
                if has_new_bar:
                    # nowait because it's used inside a regular lambda
                    self.data_queue.put_nowait(convert_ib_bar_to_dict(label_asset, bars[-1]))
            return callback

        for asset in assets:
            handle = self.ib.reqRealTimeBars(asset.to_ib_contract(), 5, IBDataServer.quote_type_map[asset.asset], False)
            handle.updateEvent += generate_callback(asset)
            self.handles[asset] = handle

    # -------------------- Order handling --------------------
    async def wait_for_fill(self, trade: ibs.Trade):
        while not trade.isDone():
            await self.ib.updateEvent

        # noinspection PyUnresolvedReferences
        return trade.orderStatus.status == 'Filled'

    async def execute(self, name: str, order: Dict[Asset, int]) -> dict:
        trades = []
        for asset, shares in order.items():
            contract = asset.to_ib_contract()
            order = ibs.MarketOrder('BUY', shares) if shares > 0 else ibs.MarketOrder('SELL', -shares)
            trades.append(self.ib.placeOrder(contract, order))

        filled = await asyncio.gather(*[self.wait_for_fill(trade) for trade in trades])
        if not all(filled):
            msg = {k: v for k, v in zip(order.keys(), filled)}
            self.logger.warning(f'Not all orders of {name} are filled: {msg}')

        executions = {}
        for trade in trades:
            # get commission
            commission = 0

            # since this trade is completed, must have at least one fill
            if trade.fills[0] == ibs.CommissionReport():
                await trade.commissionReportEvent  # make sure commission report is ready

            for fill in trade.fills:
                commission += fill.commissionReport.commission

            status = trade.orderStatus
            executions[self.contract_to_string(trade.contract)] = [status.filled, status.avgFillPrice, commission]

        return executions

    async def clean_up(self):
        self.ib.disconnect()
