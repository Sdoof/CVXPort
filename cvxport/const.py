
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


# make sure name and value are the same since we use them interchangeably
class AssetClass(Enum):
    FX = 'FX'
    STOCK = 'STOCK'


# make sure name and value are the same since we use them interchangeably
class Broker(Enum):
    IB = 'IB'
    DWX = 'DWX'  # Darwinex


class ErrorCode(Enum):
    """
    For controller response
    """
    NoIssue = 0
    AlreadyRegistered = -1
    MissingRequiredPort = -2
    NotInRegistry = -3
    UnknownRequest = -4
    UnKnownBroker = -5
