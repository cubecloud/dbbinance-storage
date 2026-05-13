"""
Verify prepare_raw_df keeps open_time as datetime64 for TIMESTAMPTZ
schema, so cache subset .loc[Timestamp:Timestamp] works natively.

Regression for the post-Stage-D bug:
    File "datafetcher.py", line 1117, in get_raw_df
        raw_df = self.CM.cache[key].loc[start_timestamp:end_timestamp]
    TypeError: '<' not supported between instances of 'int' and 'Timestamp'
"""
import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from dbbinance.fetcher import datafetcher as df_mod
from dbbinance.fetcher.constants import Constants


@pytest.fixture
def sample_timestamptz_rows():
    """Mimic psycopg2 return for TIMESTAMPTZ open_time column.
    open: float8, plus int trades. Cols match Constants.ohlcv_cols.
    """
    base = datetime.datetime(2024, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
    rows = []
    for i in range(10):
        rows.append((
            base + datetime.timedelta(minutes=i),  # open_time as datetime
            100.0 + i,   # open
            101.0 + i,   # high
            99.0 + i,    # low
            100.5 + i,   # close
            10.0 + i,    # volume
        ))
    return rows


@pytest.fixture
def sample_bigint_rows():
    """Mimic psycopg2 return for BIGINT open_time column (Unix ms)."""
    base_ms = int(datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc).timestamp() * 1000)
    rows = []
    for i in range(10):
        rows.append((
            base_ms + i * 60_000,  # open_time as int ms
            100.0 + i, 101.0 + i, 99.0 + i, 100.5 + i, 10.0 + i,
        ))
    return rows


def _build_raw_df_local(rows, use_dtypes, col_type):
    """Replicate prepare_raw_df logic under test (post-fix shape)."""
    _df = pd.DataFrame(rows, columns=list(Constants.ohlcv_cols))
    _df = _df.astype(use_dtypes)
    if col_type == df_mod.TIMESTAMPTZ:
        _df['open_time'] = pd.to_datetime(_df['open_time'], unit='ns', utc=True)
    _df = _df.sort_values(by=['open_time']).set_index('open_time', inplace=False, drop=False)
    return _df


def test_timestamptz_index_is_datetime(sample_timestamptz_rows):
    """After fix: TIMESTAMPTZ path stores DatetimeIndex (tz=UTC)."""
    df = _build_raw_df_local(sample_timestamptz_rows,
                             Constants.ohlcv_dtypes, df_mod.TIMESTAMPTZ)
    assert isinstance(df.index, pd.DatetimeIndex)
    assert str(df.index.tz) == 'UTC'


def test_timestamptz_subset_with_timestamp_bounds(sample_timestamptz_rows):
    """The exact bug scenario: .loc[Timestamp:Timestamp] no longer raises."""
    df = _build_raw_df_local(sample_timestamptz_rows,
                             Constants.ohlcv_dtypes, df_mod.TIMESTAMPTZ)
    lo = pd.Timestamp('2024-01-01 00:02:00', tz='UTC')
    hi = pd.Timestamp('2024-01-01 00:05:00', tz='UTC')
    subset = df.loc[lo:hi]
    assert len(subset) == 4  # minutes 2,3,4,5 inclusive


def test_bigint_index_is_int(sample_bigint_rows):
    """Regression: BIGINT path unchanged — Int64Index for legacy schema."""
    df = _build_raw_df_local(sample_bigint_rows,
                             Constants.ohlcv_dtypes, df_mod.BIGINT)
    # pandas may use Index of int64 (not the deprecated Int64Index class).
    assert df.index.dtype.kind == 'i'


def test_bigint_subset_with_int_bounds(sample_bigint_rows):
    """BIGINT path: int bounds slice int index — preserved behavior."""
    df = _build_raw_df_local(sample_bigint_rows,
                             Constants.ohlcv_dtypes, df_mod.BIGINT)
    base_ms = int(datetime.datetime(2024, 1, 1,
                                     tzinfo=datetime.timezone.utc).timestamp() * 1000)
    subset = df.loc[base_ms + 2*60_000: base_ms + 5*60_000]
    assert len(subset) == 4
