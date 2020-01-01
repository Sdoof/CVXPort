
import ib_insync as ibs


ib = ibs.IB()
ib.connect(port=4002)
contract = ibs.Forex('EURUSD')
handle = ib.reqRealTimeBars(contract, 5, 'MIDPOINT', True)


def on_update(bars, has_new_bar):
    if has_new_bar:
        print(bars[-1])

handle.updateEvent += on_update
ib.sleep(30)
ib.disconnect()