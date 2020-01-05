
import numpy as np
import zmq.asyncio as azmq
from typing import List

from cvxport import const, Asset, utils, Config
from cvxport.data import str_to_datum, BarPanel, EquityCurve
from cvxport.strategy import Strategy
from cvxport.worker import SatelliteWorker, startup, schedulable, service


# ==================== Data Connector ====================
class Executor(SatelliteWorker):
    """
    Simplify to only 2 type of execution strategies
    1. All or nothing
    2. Allow partial fill

    Data server should return the number of shares filled in both cases
    """
    def __init__(self, strategy: Strategy, broker: const.Broker, capital):
        super(Executor, self).__init__(f'Executor:{strategy.name}')
        self.strategy = strategy
        self.broker = broker
        self.asset_strings = [asset.string for asset in strategy.assets]
        self.positions = np.zeros(len(self.strategy.assets))

        # communication setup
        self.wait_time = Config['subscription_wait_time']
        self.port_map.update({'controller_comm_port': Config['controller_comm_port']})

        # data structure setup
        # noinspection PyTypeChecker
        self.panel = None  # type: BarPanel
        self.equity_curve = EquityCurve(strategy.assets, capital)

    # -------------------- Startup --------------------
    @startup(socket='controller_comm_port|REQ')
    async def connect(self, socket: azmq.Socket):
        """
        Ask for ports of data server
        """
        await socket.send_string(f'DataServer:{self.broker.name}')
        struct = await utils.wait_for_reply(socket, self.wait_time, const.CCode, 'Port request')  # type: dict
        ports, info = struct['ports'], struct['info']
        self.logger.info(f'Receive Data Server "{self.broker.name}" ports {ports}')
        self.logger.info(f'Receive Data Server "{self.broker.name}" info {info}')

        # setup panel
        strategy = self.strategy
        self.panel = BarPanel(strategy.assets,
                              bar_freq=strategy.freq, update_freq=const.Freq(info['freq']),
                              lookback=strategy.lookback, offset=info['offset'])

        # update port map
        self.port_map.update(ports)

    # -------------------- Set up in 2nd stage --------------------
    @schedulable(sub_socket='subscription_port|REQ', broadcast_socket='data_port|SUB')
    async def subscribe(self, sub_socket: azmq.Socket, broadcast_socket: azmq.Socket):
        """
        Send subscription request to data server and subscribe
        """
        # send subscription
        msg = ','.join(self.asset_strings)
        self.logger.info(f'Subscribing {msg}')

        await sub_socket.send_string(msg)
        await utils.wait_for_reply(sub_socket, self.wait_time, const.DCode, 'Data subscription')  # use DCode
        self.logger.info('Data subscribed')

        # subscribe to ticker
        for asset_string in self.asset_strings:
            broadcast_socket.subscribe(asset_string)

    # -------------------- Main loop to generate signal --------------------
    @service(data_socket='data_port|SUB', order_socket='order_port|REQ')
    async def run_strategy(self, data_socket: azmq.Socket, order_socket: azmq.Socket):
        bars = self.panel.update(str_to_datum(await data_socket.recv_string()))

        if bars is not None:
            timestamp, prices = bars
            new_positions = self.strategy.generate_positions(timestamp, prices)
            if new_positions is not None:
                # round towards 0 to avoid over-execution
                position_delta = np.fix(new_positions - self.positions)

                # send order
                order = {k: v for k, v in zip(self.asset_strings, position_delta) if abs(v) >= 1}
                order['Strategy'] = self.name
                order_socket.send_json(order)
                self.logger.info(f'Sent order {order}')

                executions = await utils.wait_for_reply(order_socket, self.wait_time, const.DCode, 'Order execution')

                # for post processing
                self.equity_curve.update(timestamp, executions, prices['close'][-1])

            else:
                self.equity_curve.update(timestamp, {}, prices['close'][-1])


