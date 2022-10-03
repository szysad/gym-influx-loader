from __future__ import annotations
import abc
from typing import NamedTuple
from datetime import datetime




class CoinPair(NamedTuple):
    base: str
    quote: str


class TimeRange(NamedTuple):
    _from: datetime
    _to: datetime


class Kline(NamedTuple):
    timestamp: datetime
    close: float
    high: float
    low: float
    number_of_trades: int
    open: float
    quote_asset_volume: float
    taker_buy_base_asset_volume: float
    taker_buy_base_asset_volume: float
    volume: float


class BaseLoader:
    @abc.abstractmethod
    def __iter__(self) -> BaseLoader:
        return self

    @abc.abstractmethod
    def __next__(self) -> NamedTuple:
        raise NotImplementedError
