
import abc
from typing import Iterable, List, Union
import pandas as pd
import numpy as np

from cvxport import Asset, const
from .indicator import Indicator
from .schedule import Schedule, DefaultSchedule


class Strategy(abc.ABC):
    def __init__(self, name: str, assets: List[Asset], indicators: Iterable[Indicator] = None,
                 freq: const.Freq = const.Freq.DAILY, schedule: Schedule = DefaultSchedule()):
        self.name = name
        self.assets = assets
        self.indicators = indicators if indicators else []
        # derive lookback period from indicators
        self.lookback = 0 if not indicators else max([ind.lookback for ind in indicators])
        self.freq = freq
        self.schedule = schedule

    def get_assets(self):
        return self.assets

    @abc.abstractmethod
    def _generate_positions(self, data: dict) -> np.ndarray:
        """
        Implement main strategy logic and calculation here
        """
        pass

    def generate_positions(self, timestamp: pd.Timestamp, data: dict) -> Union[np.ndarray, None]:
        for indicator in self.indicators:
            data[indicator.name] = indicator(data)

        if self.schedule(timestamp):
            return self._generate_positions(data)

        return None


class RandomStrategy(Strategy):
    def __init__(self, assets: List[Asset], freq: const.Freq, shares: int):
        super(RandomStrategy, self).__init__('RandomStrategy', assets, [], freq)
        self.shares = shares
        self.sign = -1

    def _generate_positions(self, data: dict):
        self.sign *= -1
        return np.array([self.sign * self.shares])
