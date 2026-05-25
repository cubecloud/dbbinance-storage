"""
Tests for AsyncDataFetcher.pg_resample_to_timeframe:
- output identical to resample_to_timeframe (correctness)
- cached=False does not write to cache
- second cached=True call returns identical df
- pg method is faster than pandas method (speed comparison, no assertion)
"""
import asyncio
import datetime
import time
from datetime import timezone

import pytest
import pandas as pd

from dbbinance.fetcher import Constants, create_pool, floor_time
from dbbinance.fetcher.asyncdatafetcher import AsyncDataFetcher
from dbbinance.fetcher.fetchercachemanager import FetcherCacheManager
from dbbinance.config.configpostgresql import ConfigPostgreSQL

TABLE_NAME = "spot_data_btcusdt_1m"
START_DATE_STR = '01 Aug 2018'


@pytest.fixture(scope="module")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="module")
async def pool():
    _pool = await create_pool(
        host=ConfigPostgreSQL.HOST,
        database=ConfigPostgreSQL.DATABASE,
        user=ConfigPostgreSQL.USER,
        password=ConfigPostgreSQL.PASSWORD,
    )
    yield _pool
    await _pool.close()


@pytest.fixture(scope="module")
def start_datetime():
    return datetime.datetime.strptime(START_DATE_STR, '%d %b %Y').replace(tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def end_datetime():
    return floor_time(datetime.datetime.now(timezone.utc) - datetime.timedelta(minutes=1))


def test_timeframe_to_ms():
    from dbbinance.fetcher.asyncdatafetcher import AsyncDataFetcher
    assert AsyncDataFetcher._timeframe_to_ms('1m')  == 60_000
    assert AsyncDataFetcher._timeframe_to_ms('1h')  == 3_600_000
    assert AsyncDataFetcher._timeframe_to_ms('4h')  == 14_400_000
    assert AsyncDataFetcher._timeframe_to_ms('1d')  == 86_400_000
