
import ib_insync as ibs
from datetime import datetime
import asyncio
from typing import List

from cvxport.worker import Worker, schedulable
from cvxport import Config
from cvxport.data import Asset


class Datum:
    def __init__(self, asset: Asset, date: datetime, open: float, high: float, low: float, close: float):
        self.asset = asset
        self.date = date
        self.open = open
        self.high = high
        self.low = low
        self.close = close


def convert_ib_bar_to_dict(asset: Asset, bar: ibs.BarData):
    # noinspection PyUnresolvedReferences
    return Datum(asset, bar.time, bar.open_, bar.high, bar.low, bar.close)


class IBDataServer(Worker):
    def __init__(self):
        super(IBDataServer, self).__init__('IBServer')
        self.ib = ibs.IB()
        self.ib.connect('127.0.0.1', Config['ib_port'])
        print(self.ib.positions())
        self.data_queue = asyncio.Queue()
        self.handles = {}

    # @schedulable()
    def subscribe(self):
        def onBarUpdate(bars, hasNewBar):
            print(f'hasNewBar: {hasNewBar}')
            for bar in bars:
                print(bar)

        asset = Asset('FX|EURUSD')
        handle = self.ib.reqRealTimeBars(ibs.Forex('EURUSD'), 5, 'MIDPOINT', False)
        handle.updateEvent += onBarUpdate
        self.handles[asset] = handle

    @schedulable()
    async def kill(self):
        await asyncio.sleep(10)

    def shutdown(self):
        self.ib.disconnect()


# if __name__ == '__main__':
#     server = IBDataServer()
#     server.subscribe()
#     server.run()
#
control = ibs.IB()
control.connect('127.0.0.1', 7497, clientId=10)
print(control.positions())
contract = ibs.Forex('EURUSD')

bars = control.reqHistoricalData(
        contract,
        endDateTime='',
        durationStr='100 S',
        barSizeSetting='5 secs',
        whatToShow='MIDPOINT',
        useRTH=True,
        formatDate=2,
        keepUpToDate=True)
#
# bars = control.reqRealTimeBars(contract, 5, 'MIDPOINT', False)
#
#
def onBarUpdate(bars, hasNewBar):
    if hasNewBar:
        print(bars[-1])
        # print(f'{bar.date.astimezone(timezone("EST")).strftime( "%Y-%m-%d %H:%M:%S")} {bar.open:.6f} {bar.high:.6f} {bar.low:.6f} {bar.close:.6f}')

bars.updateEvent += onBarUpdate
control.sleep(20)
control.disconnect()
