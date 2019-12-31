"""
Host
1. DataObject - API that provide data to executors
2. BarPanel - structure of ndarrays for OHLC
3. Asset class - centralized contract API
"""
import abc
import asyncio
from datetime import datetime
import zmq
import zmq.asyncio as azmq
from typing import AsyncGenerator, Tuple, Dict, List, Union
import pandas as pd
import numpy as np
import ib_insync as ib
import asyncpg as apg

from cvxport.utils import get_prices
from cvxport import const
from cvxport import Config


class Asset:
    def __init__(self, asset_string: str):
        """
        :param asset_string: asset class|ticker
        """
        self.asset, self.ticker = asset_string.split(':')
        self.asset = const.AssetClass(self.asset)  # implicitly check if asset class is valid
        self.string = asset_string

    def to_ib_contract(self):
        if self.asset == const.AssetClass.FX:
            return ib.Forex(self.ticker)
        elif self.asset == const.AssetClass.STK:
            return ib.Stock(self.ticker, exchange='SMART', currency='USD')

    def __eq__(self, other):
        return self.string == other.string

    def __hash__(self):
        return hash(self.string)

    def __repr__(self):
        return self.string


class Datum:
    def __init__(self, asset: Asset, timestamp: datetime, opn: float, high: float, low: float, close: float):
        self.asset = asset
        self.timestamp = timestamp
        self.open = opn
        self.high = high
        self.low = low
        self.close = close

    def to_df(self):
        return pd.DataFrame({'open': self.open, 'high': self.high, 'low': self.low, 'close': self.close},
                            index=[self.timestamp])

    def __str__(self):
        return f'{self.asset.string},{self.timestamp.strftime("%Y-%m-%d %H:%M:%S")},' \
               f'{self.open},{self.high},{self.low},{self.close}'


class DataStore:
    def __init__(self, broker: const.Broker, freq: const.Freq):
        self.broker = broker
        self.freq = freq
        self.table_name = f'{broker.name}_{freq.name}'.lower()
        self.con = None
        self.prepared = None

    async def connect(self):
        database = Config['postgres_db']
        user = Config['postgres_user']
        password = Config['postgres_pass']
        port = Config['postgres_port']

        self.con = await apg.connect(database=database, user=user, password=password, host='127.0.0.1', port=port)
        await self._create_table_if_not_exist()
        self.prepared = f'insert into {self.table_name} values($1, $2, $3, $4, $5, $6)'

    async def discount(self):
        await self.con.close()

    async def append(self, data: Datum):
        await self.con.execute(self.prepared,
                               data.asset.string, data.timestamp, data.open, data.high, data.low, data.close)

    async def _create_table_if_not_exist(self):
        sql = f"""
            create table if not exists {self.table_name}(
                asset varchar(16) not null,
                timestamp timestamp with time zone not null,
                open double precision not null,
                high double precision not null,
                low double precision not null,
                close double precision not null,
                primary key(asset, timestamp)
            )
        """
        await self.con.execute(sql)


class DataObject(abc.ABC):
    def __init__(self, tickers: list):
        self.tickers = tickers
        self.lookback = None
        self.freq = None
        self.N = len(tickers)

    def set_params(self, freq: const.Freq, lookback: int):
        """
        Set up frequency and lookback period. This should be set before __call__
        :param freq:
        :param lookback:
        :return:
        """
        self.freq = freq
        self.lookback = lookback

    @abc.abstractmethod
    async def __call__(self) -> AsyncGenerator[Tuple[pd.Timestamp, Dict[str, np.ndarray]], None]:
        yield


class MT4DataObject(DataObject):
    def __init__(self, tickers: list, port: int):
        super(MT4DataObject, self).__init__(tickers)

        self.port = port
        self.bar = None  # to be initialized in set_params

        # connect to server
        self.context = azmq.Context()
        # noinspection PyUnresolvedReferences
        self.in_socket = self.context.socket(zmq.SUB)
        self.in_socket.connect(f'tcp://127.0.0.1:{port}')

        # subscribe to ticket data
        for ticker in tickers:
            # noinspection PyUnresolvedReferences
            self.in_socket.setsockopt_string(zmq.SUBSCRIBE, ticker)

    def set_params(self, freq: const.Freq, lookback: int):
        super(MT4DataObject, self).set_params(freq, lookback)
        self.bar = BarPanel(self.tickers, freq, lookback)

    async def __call__(self) -> AsyncGenerator[Tuple[pd.Timestamp, Dict[str, np.ndarray]], None]:
        while True:
            msg = await self.in_socket.recv_string()
            ticker, bid_ask = msg.split(' ')
            bid, ask = bid_ask.split(';')
            mid = (float(bid) + float(ask)) / 2
            output = self.bar(pd.Timestamp.now('UTC'), ticker, mid)  # update bar data
            if output is not None:
                yield output


class OfflineDataObject(DataObject):
    def __init__(self, tickers: list, root: str, start_date: str = None, end_date: str = None):
        super(OfflineDataObject, self).__init__(tickers)

        self.data = get_prices(tickers, root_dir=root, start_date=start_date, end_date=end_date)
        self.T = self.data['close'].shape[0]

    async def __call__(self) -> AsyncGenerator[Tuple[pd.Timestamp, dict], None]:
        for idx in range(self.lookback + 1, self.N):
            start = idx - self.lookback
            # TODO: check if index and values aligned
            yield self.data['close'].index[idx], {k: v.iloc[start: idx] for k, v in self.data.items()}
            await asyncio.sleep(0)  # to yield control to other process


class BarPanel:
    """
    Extend TimedBar (single period) to store "lookback" periods of data.
    Return panel data of open, high, low, close, of size T x N, where T is lookback and N is number of tickers
    """
    def __init__(self, tickers: List[str], freq: const.Freq, lookback: int):
        self.tickers = tickers
        self.N = len(tickers)
        self.freq = freq
        self.lookback = lookback
        self.bars = TimedBars(tickers, freq)  # for a single period

        # data variables
        self.opens = np.empty((0, self.N), float)
        self.highs = np.empty((0, self.N), float)
        self.lows = np.empty((0, self.N), float)
        self.closes = np.empty((0, self.N), float)

    def __call__(self, timestamp: pd.Timestamp, ticker: str, price: float) \
            -> Union[Tuple[pd.Timestamp, Dict[str, np.ndarray]], None]:
        """
        Update tick data and return data frames of open, high, low, close when a new bar is updated.
        Otherwise, return None

        We return dict of open, high, low close here because this is how data object pass the data back to the main
        loop. Instead of making every data object packing them into dict, it's better to do it here to avoid duplicated
        code

        :param pd.Timestamp timestamp: timestamp of tick
        :param str ticker: ticker of tick
        :param float price: mid of tick
        """
        data = self.bars(timestamp, ticker, price)  # update bar data
        if data is not None:
            bar_time, opens, highs, lows, closes = data
            self.opens = np.append(self.opens, opens.reshape((1, -1)), axis=0)
            self.highs = np.append(self.highs, highs.reshape((1, -1)), axis=0)
            self.lows = np.append(self.lows, lows.reshape((1, -1)), axis=0)
            self.closes = np.append(self.closes, closes.reshape((1, -1)), axis=0)

            # keep only lookback periods of data to save memory
            if len(self.opens) > self.lookback:
                self.opens = self.opens[-self.lookback:]
                self.highs = self.highs[-self.lookback:]
                self.lows = self.lows[-self.lookback:]
                self.closes = self.closes[-self.lookback:]

            return bar_time, {'open': self.opens, 'high': self.highs, 'low': self.lows, 'close': self.closes}

        return None


class TimedBars:
    """
    Aggregate bar data of tickers for pre-defined frequency, say minutely
    """
    # TODO: TimedBars assumes at least one update within a period. However, we shouldn't trade illiquid markets

    def __init__(self, tickers: List[str], freq: const.Freq):
        self.timestamp = None
        self.tickers = tickers
        self.N = len(tickers)
        self.data = {ticker: Bar() for ticker in tickers}  # type: Dict[str, Bar]

        if freq == const.Freq.TICK:
            self.delta = pd.Timedelta(0, 'second')
            self.unit = ''
        if freq == const.Freq.MINUTE:
            self.delta = pd.Timedelta(1, 'minute')
            self.unit = 'min'
        elif freq == const.Freq.MINUTE5:
            self.delta = pd.Timedelta(5, 'minute')
            self.unit = '5min'
        elif freq == const.Freq.HOURLY:
            self.delta = pd.Timedelta(1, 'hour')
            self.unit = 'H'
        elif freq == const.Freq.DAILY:
            self.delta = pd.Timedelta(1, 'day')
            self.unit = 'D'
        elif freq == const.Freq.MONTHLY:
            self.delta = pd.Timedelta(1, 'M')
            # TODO: monthly unit is not usable in round / floor
            self.unit = 'M'

    def __call__(self, timestamp: pd.Timestamp, ticker: str, price: float) \
            -> Union[Tuple[pd.Timestamp, np.ndarray, np.ndarray, np.ndarray, np.ndarray], None]:
        """
            Update bar information and return opens, highs, lows, closes when time's up

            :param pd.Timestamp timestamp: timestamp of current tick
            :param str ticker: ticker of the tick
            :param float price: mid of the tick
            :return: None or opens, highs, lows closes when time's up
            """
        self.data[ticker].update(price)

        if self.timestamp is None:
            # use floor so that we can end this bar earlier
            self.timestamp = timestamp.floor(self.unit) if self.unit != '' else timestamp
        elif timestamp >= self.timestamp + self.delta:
            opens = np.zeros(self.N)
            highs = np.zeros(self.N)
            lows = np.zeros(self.N)
            closes = np.zeros(self.N)

            for idx, tic in enumerate(self.tickers):
                opens[idx], highs[idx], lows[idx], closes[idx] = self.data[tic].clear()

            self.timestamp = timestamp.floor(self.unit) if self.unit != '' else timestamp
            return self.timestamp, opens, highs, lows, closes

        return None


class Bar:
    def __init__(self):
        self.open = None
        self.high = None
        self.low = None
        self.close = None

    def update(self, price):
        if self.open is None:
            self.open = self.high = self.low = self.close = price

        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price

    def clear(self):
        op, high, low, close = self.open, self.high, self.low, self.close
        self.open = self.high = self.low = self.close = None
        return op, high, low, close
