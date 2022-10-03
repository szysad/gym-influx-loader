from __future__ import annotations
from src.commons import CoinPair, TimeRange, Kline
from typing import Dict, List
from influxdb_client import QueryApi


CACHE_SIZE = 2**10



class KlineInterpLoader:
    _query_rez: List[Dict[CoinPair, Kline]]
    _query_limit: int


    def __init__(
        self, coins: List[CoinPair], bucket: str, qapi: QueryApi, timerange: TimeRange, cache_size: int = CACHE_SIZE
    ) -> KlineInterpLoader:
        self._validate_params(coins, bucket, qapi, timerange)

        self._query_limit = cache_size
        self._query_rez = []
        self._bucket = bucket
        self._qapi = qapi
        self._timerange = timerange

        raise NotImplemented

    def _validate_params(
        self, coins: List[CoinPair], bucket: str, qapi: QueryApi, timerange: TimeRange
    ) -> None:
        if len(set(coins)) != len(coins):
            raise ValueError("Given coin pairs are not unique")
        
        if timerange._to <= timerange._from:
            raise ValueError("timerange must be positive")

        # TODO check if coins are in bucket
        # TODO check if in selected timerange all coins are present

    def __iter__(self) -> KlineInterpLoader:
        return self

    def __next__(self) ->  Dict[CoinPair, Kline]:
        raise NotImplementedError
