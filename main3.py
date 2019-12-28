
import ib_insync as ib
from datetime import datetime
from pytz import timezone


control = ib.IB()
control.connect('127.0.0.1', 7497, clientId=10)
print(control.positions())
contract = ib.Forex('EURUSD')
bars = control.reqHistoricalData(contract, endDateTime='', durationStr='900 S', barSizeSetting='30 secs', whatToShow='MIDPOINT',
                                 useRTH=True, formatDate=2)


def onBarUpdate(bars, hasNewBar):
    print(f'hasNewBar: {hasNewBar}')
    for bar in bars:
        print(f'{bar.date.astimezone(timezone("EST")).strftime( "%Y-%m-%d %H:%M:%S")} {bar.open:.6f} {bar.high:.6f} {bar.low:.6f} {bar.close:.6f}')

bars.updateEvent += onBarUpdate

control.sleep(60)
control.cancelHistoricalData(bars)
control.disconnect()
