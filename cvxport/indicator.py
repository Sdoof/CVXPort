
import pandas as pd
import numpy as np
import abc


class Indicator(abc.ABC):
    def __init__(self, name: str, lookback: int):
        self.name = name
        self.lookback = lookback

    @abc.abstractmethod
    def __call__(self, inputs: dict) -> np.ndarray:
        pass

    def __eq__(self, other):
        return self.name == other.name

    def __hash__(self):
        return hash(self.name)
