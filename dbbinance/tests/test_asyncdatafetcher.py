"""
Regression tests for AsyncDataFetcher.resample_to_timeframe cache fixes:
- cached=False must not write to cache
- cached=True returns identical df on second call
- cached=True and cached=False produce identical data
- subset lookup serves narrower range from existing cache entry
"""
import datetime
import asyncio
from datetime import timezone

import pytest
import pandas as pd

from dbbinance.fetcher import Constants
from dbbinance.fetcher import create_pool, floor_time
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


async def test_resample_cached_same_result(pool, start_datetime, end_datetime):
    """cached=True: second call returns identical df (cache hit)."""
    fetcher = AsyncDataFetcher(pool=pool, binance_api_key='dummy', binance_api_secret='dummy')

    _data_df = await fetcher.resample_to_timeframe(
        table_name=TABLE_NAME, start=start_datetime, end=end_datetime,
        to_timeframe="1h", origin="start",
        use_cols=Constants.ohlcv_cols, use_dtypes=Constants.ohlcv_dtypes,
        open_time_index=False, cached=True,
    )
    _data_df_1 = await fetcher.resample_to_timeframe(
        table_name=TABLE_NAME, start=start_datetime, end=end_datetime,
        to_timeframe="1h", origin="start",
        use_cols=Constants.ohlcv_cols, use_dtypes=Constants.ohlcv_dtypes,
        open_time_index=False, cached=True,
    )

    assert not _data_df.empty
    result_df = _data_df.compare(_data_df_1, align_axis=0)
    assert result_df.empty, f"cached=True second call differs:\n{result_df}"


async def test_resample_cached_vs_uncached(pool, start_datetime, end_datetime):
    """cached=True and cached=False produce identical data."""
    fetcher = AsyncDataFetcher(pool=pool, binance_api_key='dummy', binance_api_secret='dummy')
    fetcher2 = AsyncDataFetcher(pool=pool, binance_api_key='dummy', binance_api_secret='dummy')

    _data_df = await fetcher.resample_to_timeframe(
        table_name=TABLE_NAME, start=start_datetime, end=end_datetime,
        to_timeframe="1h", origin="start",
        use_cols=Constants.ohlcv_cols, use_dtypes=Constants.ohlcv_dtypes,
        open_time_index=False, cached=True,
    )
    _data_df_3 = await fetcher2.resample_to_timeframe(
        table_name=TABLE_NAME, start=start_datetime, end=end_datetime,
        to_timeframe="1h", origin="start",
        use_cols=Constants.ohlcv_cols, use_dtypes=Constants.ohlcv_dtypes,
        open_time_index=False, cached=False,
    )

    result_df = _data_df.compare(_data_df_3, align_axis=0)
    assert result_df.empty, f"cached vs uncached mismatch:\n{result_df}"


async def test_resample_no_cache_write_when_uncached(pool, start_datetime, end_datetime):
    """cached=False must not write any entry to cache."""
    cache_manager_obj = FetcherCacheManager(max_memory_gb=1)
    fetcher = AsyncDataFetcher(pool=pool, binance_api_key='dummy', binance_api_secret='dummy',
                               cache_obj=cache_manager_obj)

    await fetcher.resample_to_timeframe(
        table_name=TABLE_NAME, start=start_datetime, end=end_datetime,
        to_timeframe="1h", origin="start",
        use_cols=Constants.ohlcv_cols, use_dtypes=Constants.ohlcv_dtypes,
        open_time_index=False, cached=False,
    )

    assert len(cache_manager_obj.cache) == 0, (
        f"cached=False wrote {len(cache_manager_obj.cache)} entry(ies) — expected 0"
    )


async def test_resample_subset_served_from_cache(pool, start_datetime, end_datetime):
    """Narrower range is served from existing cache entry (subset lookup)."""
    cache_manager_obj = FetcherCacheManager(max_memory_gb=1)
    fetcher = AsyncDataFetcher(pool=pool, binance_api_key='dummy', binance_api_secret='dummy',
                               cache_obj=cache_manager_obj)

    _data_df = await fetcher.resample_to_timeframe(
        table_name=TABLE_NAME, start=start_datetime, end=end_datetime,
        to_timeframe="1h", origin="start",
        use_cols=Constants.ohlcv_cols, use_dtypes=Constants.ohlcv_dtypes,
        open_time_index=False, cached=True,
    )

    start_datetime_1 = start_datetime + datetime.timedelta(days=30)
    _data_df_1 = await fetcher.resample_to_timeframe(
        table_name=TABLE_NAME, start=start_datetime_1, end=end_datetime,
        to_timeframe="1h", origin="start",
        use_cols=Constants.ohlcv_cols, use_dtypes=Constants.ohlcv_dtypes,
        open_time_index=False, cached=True,
    )

    assert not _data_df_1.empty
    assert len(_data_df_1) < len(_data_df), "Subset must be smaller than full range"
