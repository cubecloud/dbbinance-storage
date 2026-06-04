"""last_full_bar (v0.97): the resampler drops the incomplete trailing bin (complete bars only).

For an `end` that falls mid-bin (e.g. end=11:00 on a 30m grid -> the [11:00,11:30) bin holds
only the 11:00 minute = partial), last_full_bar=True must drop it (last bar 10:30); =False keeps
it (last bar 11:00). Complete bars must be byte-identical between the two. Covers both the SQL
(pg_resample_to_timeframe) and pandas (resample_to_timeframe) paths.

Needs the real DB (PG creds). Run:
  set -a; for f in *.env (sunday repo); do source $f; done; set +a
  conda run -n sunday-base-213-tests python -m pytest \
    /home/cubecloud/Python/projects/dbbinance-storage/tests/test_last_full_bar.py -v
"""
import os
import datetime as dt

import pandas as pd
import pytest

_HAS_DB = bool(os.environ.get('PSGSQL_KEY') and os.environ.get('PSGSQLPARSE_USERNAME'))
pytestmark = pytest.mark.skipif(not _HAS_DB, reason='needs PG creds in env')

OHLCV = ('open', 'high', 'low', 'close', 'volume')
START = dt.datetime(2024, 5, 20, 0, 0, tzinfo=dt.timezone.utc)
END_MIDBIN = dt.datetime(2024, 5, 23, 11, 0, tzinfo=dt.timezone.utc)   # 30m: [11:00,11:30) partial
LAST_FULL = pd.Timestamp('2024-05-23 10:30', tz='UTC')
LAST_PARTIAL = pd.Timestamp('2024-05-23 11:00', tz='UTC')


@pytest.fixture(scope='module')
def fetcher():
    from dbbinance.config.configpostgresql import ConfigPostgreSQL
    from dbbinance.fetcher.datafetcher import DataFetcher
    return DataFetcher(host=ConfigPostgreSQL.HOST, database=ConfigPostgreSQL.DATABASE,
                       user=ConfigPostgreSQL.USER, password=ConfigPostgreSQL.PASSWORD,
                       binance_api_key='dummy', binance_api_secret='dummy')


def _load(fetcher, method, last_full_bar):
    df = getattr(fetcher, method)(
        table_name='spot_data_btcusdt_1m', start=START, end=END_MIDBIN, to_timeframe='30m',
        origin='start', use_cols=('open_time', *OHLCV),
        use_dtypes={c: 'float64' for c in OHLCV}, open_time_index=False,
        last_full_bar=last_full_bar)
    df['open_time'] = pd.to_datetime(df['open_time'], utc=True)
    return df.set_index('open_time')[list(OHLCV)].sort_index()


@pytest.mark.parametrize('method', ['pg_resample_to_timeframe', 'resample_to_timeframe'])
def test_last_full_bar_drops_partial(fetcher, method):
    full = _load(fetcher, method, last_full_bar=True)
    partial = _load(fetcher, method, last_full_bar=False)
    # True -> last complete bar; False -> includes the partial 11:00 bin.
    assert full.index[-1] == LAST_FULL, f'{method}: last_full_bar=True last={full.index[-1]}'
    assert partial.index[-1] == LAST_PARTIAL, f'{method}: last_full_bar=False last={partial.index[-1]}'
    # Exactly one extra (partial) bar in the False result; complete bars identical.
    assert len(partial) == len(full) + 1
    common = full.index
    for c in OHLCV:
        d = (full.loc[common, c] - partial.loc[common, c]).abs()
        assert (d <= 1e-6).all(), f'{method} {c}: complete bars differ (max {float(d.max())})'


def test_default_is_last_full_bar_true(fetcher):
    """Default (no kwarg) must equal last_full_bar=True (complete bars only)."""
    default = fetcher.pg_resample_to_timeframe(
        table_name='spot_data_btcusdt_1m', start=START, end=END_MIDBIN, to_timeframe='30m',
        origin='start', use_cols=('open_time', *OHLCV),
        use_dtypes={c: 'float64' for c in OHLCV}, open_time_index=False)
    last = pd.to_datetime(default['open_time'], utc=True).iloc[-1]
    assert last == LAST_FULL, f'default last bar {last} != {LAST_FULL} (default should drop partial)'
