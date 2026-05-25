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


async def test_pg_resample_identical_to_pandas(pool, start_datetime, end_datetime):
    """pg_resample_to_timeframe output must be identical to resample_to_timeframe."""
    fetcher = AsyncDataFetcher(pool=pool, binance_api_key='dummy', binance_api_secret='dummy')

    pandas_df = await fetcher.resample_to_timeframe(
        table_name=TABLE_NAME, start=start_datetime, end=end_datetime,
        to_timeframe="1h", origin="start",
        use_cols=Constants.ohlcv_cols, use_dtypes=Constants.ohlcv_dtypes,
        open_time_index=True, cached=False,
    )
    pg_df = await fetcher.pg_resample_to_timeframe(
        table_name=TABLE_NAME, start=start_datetime, end=end_datetime,
        to_timeframe="1h", origin="start",
        use_cols=Constants.ohlcv_cols, use_dtypes=Constants.ohlcv_dtypes,
        open_time_index=True, cached=False,
    )

    assert not pandas_df.empty, "pandas result is empty"
    assert not pg_df.empty, "pg result is empty"
    assert list(pandas_df.columns) == list(pg_df.columns), \
        f"column mismatch: pandas={list(pandas_df.columns)} pg={list(pg_df.columns)}"
    assert len(pandas_df) == len(pg_df), \
        f"row count mismatch: pandas={len(pandas_df)} pg={len(pg_df)}"

    # SUM() order differs between PG and pandas — allow tiny float noise, catch real errors.
    # check_freq=False: pandas resample sets freq=Hour on index; pg result has None.
    pd.testing.assert_frame_equal(pandas_df, pg_df, rtol=1e-9, check_freq=False)


async def test_pg_resample_no_cache_write_when_uncached(pool, start_datetime, end_datetime):
    """cached=False must not write any entry to the cache."""
    cache_manager_obj = FetcherCacheManager(max_memory_gb=1)
    fetcher = AsyncDataFetcher(
        pool=pool, binance_api_key='dummy', binance_api_secret='dummy',
        cache_obj=cache_manager_obj,
    )

    await fetcher.pg_resample_to_timeframe(
        table_name=TABLE_NAME, start=start_datetime, end=end_datetime,
        to_timeframe="1h", origin="start",
        use_cols=Constants.ohlcv_cols, use_dtypes=Constants.ohlcv_dtypes,
        open_time_index=True, cached=False,
    )

    assert len(cache_manager_obj.cache) == 0, (
        f"cached=False wrote {len(cache_manager_obj.cache)} entry(ies) to cache"
    )


async def test_pg_resample_cached_same_result(pool, start_datetime, end_datetime):
    """cached=True: second call returns identical df (cache hit)."""
    fetcher = AsyncDataFetcher(pool=pool, binance_api_key='dummy', binance_api_secret='dummy')

    first_df = await fetcher.pg_resample_to_timeframe(
        table_name=TABLE_NAME, start=start_datetime, end=end_datetime,
        to_timeframe="1h", origin="start",
        use_cols=Constants.ohlcv_cols, use_dtypes=Constants.ohlcv_dtypes,
        open_time_index=True, cached=True,
    )
    second_df = await fetcher.pg_resample_to_timeframe(
        table_name=TABLE_NAME, start=start_datetime, end=end_datetime,
        to_timeframe="1h", origin="start",
        use_cols=Constants.ohlcv_cols, use_dtypes=Constants.ohlcv_dtypes,
        open_time_index=True, cached=True,
    )

    assert not first_df.empty
    diff = first_df.compare(second_df, align_axis=0)
    assert diff.empty, f"cached=True second call differs:\n{diff}"


async def test_speed_comparison(pool, start_datetime, end_datetime):
    """Compare wall-clock time: pg vs pandas. Alternating runs, no assertion."""
    fetcher = AsyncDataFetcher(pool=pool, binance_api_key='dummy', binance_api_secret='dummy')
    N = 3

    # warm-up
    await fetcher.resample_to_timeframe(
        table_name=TABLE_NAME, start=start_datetime, end=end_datetime,
        to_timeframe="1h", origin="start",
        use_cols=Constants.ohlcv_cols, use_dtypes=Constants.ohlcv_dtypes,
        open_time_index=True, cached=False,
    )
    await fetcher.pg_resample_to_timeframe(
        table_name=TABLE_NAME, start=start_datetime, end=end_datetime,
        to_timeframe="1h", origin="start",
        use_cols=Constants.ohlcv_cols, use_dtypes=Constants.ohlcv_dtypes,
        open_time_index=True, cached=False,
    )

    pandas_times = []
    pg_times = []
    for _ in range(N):
        t0 = time.perf_counter()
        await fetcher.resample_to_timeframe(
            table_name=TABLE_NAME, start=start_datetime, end=end_datetime,
            to_timeframe="1h", origin="start",
            use_cols=Constants.ohlcv_cols, use_dtypes=Constants.ohlcv_dtypes,
            open_time_index=True, cached=False,
        )
        pandas_times.append((time.perf_counter() - t0) * 1000)

        t0 = time.perf_counter()
        await fetcher.pg_resample_to_timeframe(
            table_name=TABLE_NAME, start=start_datetime, end=end_datetime,
            to_timeframe="1h", origin="start",
            use_cols=Constants.ohlcv_cols, use_dtypes=Constants.ohlcv_dtypes,
            open_time_index=True, cached=False,
        )
        pg_times.append((time.perf_counter() - t0) * 1000)

    pandas_avg = sum(pandas_times) / N
    pg_avg = sum(pg_times) / N
    speedup = pandas_avg / pg_avg if pg_avg > 0 else float('inf')

    print(f"\n{'Method':<25} {'Run 1':>8} {'Run 2':>8} {'Run 3':>8} {'Avg':>8}")
    print("-" * 60)
    print(f"{'resample_to_timeframe':<25} " +
          " ".join(f"{t:>7.0f}ms" for t in pandas_times) +
          f" {pandas_avg:>7.0f}ms")
    print(f"{'pg_resample_to_timeframe':<25} " +
          " ".join(f"{t:>7.0f}ms" for t in pg_times) +
          f" {pg_avg:>7.0f}ms")
    print(f"\nSpeedup: {speedup:.2f}x  ({'faster' if speedup > 1 else 'slower'})")


# ---------------------------------------------------------------------------
# Multi-timeframe correctness tests
# ---------------------------------------------------------------------------

# All timeframes that both methods support.
# '1w' excluded: pandas resample('1W') uses Sunday-anchored bins regardless of origin,
# which cannot be reproduced with a start-aligned SQL formula.
_TEST_TIMEFRAMES = [timeframe for timeframe in Constants.time_intervals if timeframe in Constants.binsizes and timeframe != '1w']

# Fixed 7-day window with good historical data, start aligned to midnight
_WINDOW_START = datetime.datetime(2021, 1, 4, 0, 0, 0, tzinfo=timezone.utc)
_WINDOW_END   = datetime.datetime(2021, 1, 11, 0, 0, 0, tzinfo=timezone.utc)


@pytest.mark.parametrize("timeframe", _TEST_TIMEFRAMES)
async def test_pg_resample_all_timeframes(pool, timeframe):
    """pg output must match pandas output for every timeframe in Constants."""
    fetcher = AsyncDataFetcher(pool=pool, binance_api_key='dummy', binance_api_secret='dummy')

    pandas_df = await fetcher.resample_to_timeframe(
        table_name=TABLE_NAME, start=_WINDOW_START, end=_WINDOW_END,
        to_timeframe=timeframe, origin="start",
        use_cols=Constants.ohlcv_cols, use_dtypes=Constants.ohlcv_dtypes,
        open_time_index=True, cached=False,
    )
    pg_df = await fetcher.pg_resample_to_timeframe(
        table_name=TABLE_NAME, start=_WINDOW_START, end=_WINDOW_END,
        to_timeframe=timeframe, origin="start",
        use_cols=Constants.ohlcv_cols, use_dtypes=Constants.ohlcv_dtypes,
        open_time_index=True, cached=False,
    )

    assert not pandas_df.empty, f"{timeframe}: pandas result empty"
    assert not pg_df.empty,     f"{timeframe}: pg result empty"
    try:
        pd.testing.assert_frame_equal(pandas_df, pg_df, rtol=1e-9, check_freq=False)
    except AssertionError as e:
        raise AssertionError(f"timeframe={timeframe}: {e}") from None


# ---------------------------------------------------------------------------
# Boundary-start correctness tests
# Each case: start offset N minutes from a natural freq boundary.
# Tests that origin='start' binning is correct at freq-edge conditions.
# ---------------------------------------------------------------------------

_BOUNDARY_CASES = [
    # (timeframe, offset_min)         description
    ("1h",    0),   # aligned to hour
    ("1h",    1),   # +1 min: first bin has 1 candle
    ("1h",   59),   # +(freq-1) min: first bin has 1 candle
    ("4h",    0),
    ("4h",    1),
    ("4h",  239),   # +(4*60-1) min
    ("1d",    0),
    ("1d",    1),
    ("1d", 1439),   # +(24*60-1) min
    ("15m",   0),
    ("15m",   1),
    ("15m",  14),   # +(15-1) min
    ("30m",   0),
    ("30m",   1),
    ("30m",  29),   # +(30-1) min
]


@pytest.mark.parametrize("timeframe,offset_min", _BOUNDARY_CASES)
async def test_pg_resample_boundary_starts(pool, timeframe, offset_min):
    """pg and pandas must agree when start is offset from natural freq boundaries."""
    start = _WINDOW_START + datetime.timedelta(minutes=offset_min)
    end   = start + datetime.timedelta(days=7)

    fetcher = AsyncDataFetcher(pool=pool, binance_api_key='dummy', binance_api_secret='dummy')

    pandas_df = await fetcher.resample_to_timeframe(
        table_name=TABLE_NAME, start=start, end=end,
        to_timeframe=timeframe, origin="start",
        use_cols=Constants.ohlcv_cols, use_dtypes=Constants.ohlcv_dtypes,
        open_time_index=True, cached=False,
    )
    pg_df = await fetcher.pg_resample_to_timeframe(
        table_name=TABLE_NAME, start=start, end=end,
        to_timeframe=timeframe, origin="start",
        use_cols=Constants.ohlcv_cols, use_dtypes=Constants.ohlcv_dtypes,
        open_time_index=True, cached=False,
    )

    try:
        pd.testing.assert_frame_equal(pandas_df, pg_df, rtol=1e-9, check_freq=False)
    except AssertionError as e:
        raise AssertionError(
            f"timeframe={timeframe}, offset={offset_min}min: {e}"
        ) from None
