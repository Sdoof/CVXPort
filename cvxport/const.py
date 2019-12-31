
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
    MOCK = 'MOCK'  # for testing purpose
    IB = 'IB'
    DWX = 'DWX'  # Darwinex


class CCode(Enum):
    """
    Code for controller
    """
    Succeeded = 0
    AlreadyRegistered = -1
    MissingRequiredPort = -2
    NotInRegistry = -3
    UnknownRequest = -4
    UnKnownBroker = -5
    ServerNotOnline = -6


class DCode(Enum):
    """
    Code for data server
    """
    Succeeded = 0
