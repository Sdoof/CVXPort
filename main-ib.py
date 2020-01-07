
import ib_insync as ibs
import asyncio


async def wait_for_fill(ib, trade):
    print(f'Waiting for fill {trade.contract.symbol}')
    while not trade.isDone():
        # print(f'{trade.contract.pair()} remains {trade.remaining()}')
        await ib.updateEvent
    return trade


def show(bars, has_new_bar):
    if has_new_bar:
        print(bars[-1])


async def main():
    tickers = ['EURUSD']
    contracts = []
    ib = ibs.IB()
    await ib.connectAsync(port=4002, clientId=2)

    print('----------- Starting Positions -----------')
    print(ib.positions())
    # [ib.cancelOrder(order) for order in ib.openOrders()]
    # print(ib.openOrders())
    # print(ib.openTrades())

    for ticker in tickers:
        con = ibs.Forex(ticker)
        await ib.qualifyContractsAsync(con)
        contracts.append(con)

    bars = ib.reqRealTimeBars(contracts[0], 5, 'MIDPOINT', True)
    bars.updateEvent += show

    def print_yes():
        print('yes!')

    ib.updateEvent += print_yes

    # for i in range(2):
    #     trades = []
    #
    #     print('\n----------- New Trades -----------')
    #     for contract in contracts:
    #         order = ibs.MarketOrder('BUY', 10000) if i % 2 == 0 else ibs.MarketOrder('SELL', 10000)
    #         trade = ib.placeOrder(contract, order)
    #         trades.append(trade)
    #
    #     status = await asyncio.gather(*[wait_for_fill(ib, trade) for trade in trades])
    #     for trade in trades:
    #         print(trade)
    #         print(f'{trade.contract.symbol}: {trade.orderStatus.avgFillPrice}')
    #         if trade.fills[0].commissionReport == ibs.CommissionReport():
    #             await trade.commissionReportEvent
    #
    #         for fill in trade.fills:
    #             print(fill.commissionReport)
    #
    #     print('Positions After Fill ...')
    #     print(ib.positions())
    #
    #     # for fill in ib.fills():
    #
    #     await asyncio.sleep(5)

    await asyncio.sleep(700)
    # ib.cancelRealTimeBars(bars)
    ib.disconnect()

asyncio.run(main())


# ib = ibs.IB()
# ib.connect(port=4002, clientId=2)
# con = ibs.Forex('EURUSD')
# ib.reqRealTimeBars(con, 5, 'MIDPOINT', False)
# ib.sleep(5)
# ib.disconnect()