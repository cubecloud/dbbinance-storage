"""Tests for the use_extended_cols option on (pg_)resample_to_timeframe (sync + async).

DB-free: _resolve_use_cols helper (both fetcher classes) + Constants.binance_extended_cols.
DB-backed (real PG, mirrors test_pg_resample_to_timeframe.py): extended resample returns the
order-flow columns, pg == pandas (this also verifies the default_agg_dict short-name fix — a
missing agg would make the extended SUM break/diverge), taker_buy_base <= volume sanity, and
use_extended_cols=False stays OHLCV-only (back-compat).
"""
import asyncio
import datetime
from datetime import timezone

import pytest
import pandas as pd

from dbbinance.fetcher import Constants, create_pool, floor_time
from dbbinance.fetcher.asyncdatafetcher import AsyncDataFetcher
from dbbinance.fetcher.datafetcher import DataFetcher
from dbbinance.fetcher.fetchercachemanager import FetcherCacheManager
from dbbinance.config.configpostgresql import ConfigPostgreSQL

TABLE_NAME = "spot_data_btcusdt_1m"
_EXTRA_COLS = ('quote_asset_volume', 'trades', 'taker_buy_base', 'taker_buy_quote')


# --- DB-free: helper + constants ------------------------------------------------------------

def test_constants_extended_cols():
    cols = Constants.binance_extended_cols
    # OHLCV first, then the order-flow fields; close_time / ignored excluded.
    assert cols[:6] == Constants.ohlcv_cols
    for extra in _EXTRA_COLS:
        assert extra in cols, f"{extra} missing from binance_extended_cols"
    assert 'close_time' not in cols and 'ignored' not in cols
    # dtypes cover every column, trades is int
    assert set(Constants.binance_extended_dtypes) == set(cols)
    assert Constants.binance_extended_dtypes['trades'] is int


@pytest.mark.parametrize("fetcher_cls", [DataFetcher, AsyncDataFetcher])
def test_resolve_use_cols(fetcher_cls):
    """True -> extended set (overrides use_cols); False -> use_cols unchanged."""
    assert fetcher_cls._resolve_use_cols(Constants.ohlcv_cols, True) == Constants.binance_extended_cols
    assert fetcher_cls._resolve_use_cols(Constants.ohlcv_cols, False) == Constants.ohlcv_cols
    # custom use_cols passes through untouched when off
    custom = ('open_time', 'close')
    assert fetcher_cls._resolve_use_cols(custom, False) == custom


# --- DB-backed: real resample of the extended columns ---------------------------------------

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


_WINDOW_START = datetime.datetime(2021, 1, 4, 0, 0, 0, tzinfo=timezone.utc)
_WINDOW_END = datetime.datetime(2021, 1, 11, 0, 0, 0, tzinfo=timezone.utc)


async def test_extended_cols_present_and_pg_matches_pandas(pool):
    """use_extended_cols=True yields the order-flow columns, identical pg vs pandas (agg fix)."""
    fetcher = AsyncDataFetcher(pool=pool, binance_api_key='dummy', binance_api_secret='dummy')
    kwargs = dict(table_name=TABLE_NAME, start=_WINDOW_START, end=_WINDOW_END,
                  to_timeframe="1h", origin="start",
                  use_dtypes=Constants.binance_extended_dtypes,
                  open_time_index=True, cached=False, use_extended_cols=True)

    pandas_df = await fetcher.resample_to_timeframe(**kwargs)
    pg_df = await fetcher.pg_resample_to_timeframe(**kwargs)

    assert not pandas_df.empty and not pg_df.empty
    for extra in _EXTRA_COLS:
        assert extra in pandas_df.columns, f"{extra} missing (pandas) — agg/use_cols not applied"
        assert extra in pg_df.columns, f"{extra} missing (pg)"
    # a missing agg would diverge here; equality proves the short-name agg fix works end-to-end
    pd.testing.assert_frame_equal(pandas_df, pg_df, rtol=1e-9, check_freq=False)


async def test_extended_taker_buy_not_exceeding_volume(pool):
    """Sanity: aggregated taker_buy_base <= volume per bar (taker buys are a subset of volume)."""
    fetcher = AsyncDataFetcher(pool=pool, binance_api_key='dummy', binance_api_secret='dummy')
    df = await fetcher.pg_resample_to_timeframe(
        table_name=TABLE_NAME, start=_WINDOW_START, end=_WINDOW_END,
        to_timeframe="1h", origin="start", open_time_index=True, cached=False,
        use_extended_cols=True)
    assert (df['taker_buy_base'] <= df['volume'] + 1e-6).all(), "taker_buy_base exceeds volume"
    assert (df['trades'] > 0).all(), "non-positive trades in a populated window"


async def test_extended_vs_ohlcv_cache_no_collision(pool):
    """Same window cached BOTH ways must not collide (cache key includes use_extended_cols).

    Regression: previously the cache key omitted use_cols/use_extended_cols, so an OHLCV load
    (cached) and an extended load of the same window returned the same cached 6-column frame.
    """
    cache = FetcherCacheManager(max_memory_gb=1)
    fetcher = AsyncDataFetcher(pool=pool, binance_api_key='dummy', binance_api_secret='dummy',
                              cache_obj=cache)
    common = dict(table_name=TABLE_NAME, start=_WINDOW_START, end=_WINDOW_END,
                  to_timeframe="1h", origin="start", open_time_index=True, cached=True)

    ohlcv = await fetcher.pg_resample_to_timeframe(
        **common, use_cols=Constants.ohlcv_cols, use_dtypes=Constants.ohlcv_dtypes,
        use_extended_cols=False)
    ext = await fetcher.pg_resample_to_timeframe(
        **common, use_dtypes=Constants.binance_extended_dtypes, use_extended_cols=True)

    assert 'taker_buy_base' not in ohlcv.columns
    assert 'taker_buy_base' in ext.columns, "extended load returned the cached OHLCV frame (collision)"


async def test_default_off_is_ohlcv_only(pool):
    """use_extended_cols=False (default) keeps the OHLCV-only output (back-compat)."""
    fetcher = AsyncDataFetcher(pool=pool, binance_api_key='dummy', binance_api_secret='dummy')
    df = await fetcher.pg_resample_to_timeframe(
        table_name=TABLE_NAME, start=_WINDOW_START, end=_WINDOW_END,
        to_timeframe="1h", origin="start",
        use_cols=Constants.ohlcv_cols, use_dtypes=Constants.ohlcv_dtypes,
        open_time_index=True, cached=False)
    assert list(df.columns) == [c for c in Constants.ohlcv_cols if c != 'open_time']
    for extra in _EXTRA_COLS:
        assert extra not in df.columns
