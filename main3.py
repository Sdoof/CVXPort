
import ib_insync as ibs
from datetime import datetime
import asyncio
from typing import List

from cvxport.worker import Worker, schedulable
from cvxport import Config
from cvxport.data import Asset


# control = ibs.IB()
# control.connect('127.0.0.1', 7497, clientId=10)
# print(control.positions())
#
# # bars = control.reqHistoricalData(
# #         contract,
# #         endDateTime='',
# #         durationStr='100 S',
# #         barSizeSetting='5 secs',
# #         whatToShow='MIDPOINT',
# #         useRTH=True,
# #         formatDate=2,
# #         keepUpToDate=True)
#
# bars = control.reqRealTimeBars(contract, 5, 'MIDPOINT', False)
#

# bars.updateEvent += onBarUpdate
# control.sleep(20)
# control.disconnect()

def onBarUpdate(bars, hasNewBar):
    if hasNewBar:
        print(bars[-1])
        # print(f'{bar.date.astimezone(timezone("EST")).strftime( "%Y-%m-%d %H:%M:%S")} {bar.open:.6f} {bar.high:.6f} {bar.low:.6f} {bar.close:.6f}')


async def main():
    ib = ibs.IB()
    await ib.connectAsync()
    contract = ibs.Forex('EURUSD')
    bars = ib.reqRealTimeBars(contract, 5, 'MIDPOINT', False)
    bars.updateEvent += onBarUpdate
    await asyncio.sleep(20)
    ib.disconnect()

asyncio.run(main())