
import abc
from typing import Iterable
import pandas as pd
from .indicator import Indicator
from .schedule import Schedule, DefaultSchedule
from .const import Freq


class Strategy(abc.ABC):
    def __init__(self, name: str, indicators: Iterable[Indicator] = None, freq: str = Freq.DAILY,
                 schedule: Schedule = DefaultSchedule()):
        self.name = name
        self.assets = None
        self.indicators = indicators if indicators is not None else []
        # derive lookback period from indicators
        self.lookback = 0 if indicators is None else max([ind.lookback for ind in indicators])
        self.freq = freq
        self.schedule = schedule

    def get_assets(self):
        return self.assets

    @abc.abstractmethod
    def calculate_position(self, data: dict):
        """
        Implement main strategy logic and calculation here
        """
        pass

    def __call__(self, timestamp: pd.Timestamp, data: dict):
        for indicator in self.indicators:
            data[indicator.name] = indicator(data)

        if self.schedule(timestamp):
            return self.calculate_position(data)
        return None


class RandomStrategy(Strategy):
    def calculate_position(self, data: dict):
        pass
