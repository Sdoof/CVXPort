
import abc
import asyncio
import numpy as np
from .data import DataObject
from .strategy import Strategy


class Executor(abc.ABC):
    def __init__(self, strategy: Strategy, data: DataObject, data_params: dict):
        self.strategy = strategy
        self.data = data
        self.data.set_params(strategy.freq, strategy.lookback)  # must set up these before using DataObject
        self.execution_info_queue = asyncio.Queue()

    def run(self):
        asyncio.run(self._run_all())

    # ==================== To Override ====================
    @abc.abstractmethod
    async def _execute_position(self, shares) -> dict:
        """
        Execute orders as specified per "shares"

        :param np.ndarray shares:
        :return: information of execution
        """
        pass

    @abc.abstractmethod
    async def _get_current_position(self) -> np.ndarray:
        """
        Return current open position

        :return: current open position
        """
        pass

    # ==================== Routines ====================
    async def _run_strategy(self):
        """
        Main executor loop. Ingest data into strategy and execute position per strategy calculation
        """
        async for timestamp, subset in self.data():
            new_position = self.strategy(timestamp, subset)
            if new_position is not None:  # strategy may not generate position for each data update
                curr_position = await self._get_current_position()

                # round towards 0 to avoid overflow
                position_delta = np.fix(new_position - curr_position)

                # shouldn't use queue for execution because in backtest we need instant execution
                execution_info = await self._execute_position(position_delta)

                # for post processing
                await self.execution_info_queue.put(execution_info)

    async def _process_execution_info(self):
        """
        Consume execution info from queue and post-process the info
        """
        while True:
            info = await self.execution_info_queue.get()

    async def _process_request(self):
        """
        Handle requests from zeromq
        """
        while True:
            a = 1

    async def _run_all(self):
        """
        Start all the routines
        """
        self.strategy_daemon = self._run_strategy()  # get handle so that we can cancel the coroutine later
        self.info_daemon = self._process_execution_info()  # get handle so that we can cancel the coroutine later
        # _process_request is the main controller. no need for handle
        await asyncio.gather(self._process_request(), self.strategy_daemon, self.info_daemon)
