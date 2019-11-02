
from enum import Enum


class Freq(Enum):
    """
    Use datetime format code here
    """
    MONTHLY = 'm'
    DAILY = 'd'
    HOURLY = 'H'
    MINUTE = 'M'
    MINUTE5 = '5M'
    TICK = 'T'  # for testing only. We don't have the infra to trade on tick
