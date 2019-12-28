
import ib_insync as ib


control = ib.IB()
control.connect('127.0.0.1', 7497, clientId=10)
print(control.positions())
contract = ib.Forex('EURUSD')
bars = control.reqHistoricalData(contract, endDateTime='', durationStr='10 D', barSizeSetting='1 hour', whatToShow='TRADES',
                                 useRTH=True, formatDate=1)
control.reqHistoricalData()
control.disconnect()


