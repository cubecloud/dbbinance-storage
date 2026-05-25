"""
DB-free tests for the synchronous DataRepair (datafetcher.py).

No PostgreSQL and no Binance API are required: the input DataFrames are built
to match exactly what `get_all_data_as_df` returns, and the Binance client is a
local fake. These reproduce:
  - #1: the TIMESTAMPTZ-schema regression (open_time arrives as tz-aware
        datetime, but before_preparation re-applies unit='ms')
  - #3: the repair_index call-site bug (return value is a 3-tuple, used as a df)
and pin the behaviour of the pure helpers.
"""
import datetime
from datetime import timezone

import numpy as np
import pandas as pd
import pytest

from dbbinance.fetcher import Constants
from dbbinance.fetcher.datafetcher import DataRepair


def _make_sql_df(index) -> pd.DataFrame:
    """Build a DataFrame shaped like get_all_data_as_df output: sql_cols columns,
    open_time/close_time as datetime64[ns, UTC] (TIMESTAMPTZ schema)."""
    n = len(index)
    df = pd.DataFrame({
        "id": np.arange(1, n + 1, dtype="int64"),
        "open_time": index,
        "open": np.arange(n, dtype=float) + 1,
        "high": np.arange(n, dtype=float) + 2,
        "low": np.arange(n, dtype=float) + 0.5,
        "close": np.arange(n, dtype=float) + 1.5,
        "volume": np.arange(n, dtype=float) + 1,
        "close_time": index,
        "quote_asset_volume": np.zeros(n, dtype=float),
        "trades": np.zeros(n, dtype="int64"),
        "taker_buy_base": np.zeros(n, dtype=float),
        "taker_buy_quote": np.zeros(n, dtype=float),
        "ignored": np.zeros(n, dtype=float),
    })
    df["open_time"] = df["open_time"].astype("datetime64[ns, UTC]")
    df["close_time"] = df["close_time"].astype("datetime64[ns, UTC]")
    return df


# ---------------------------------------------------------------- #1 schema

def test_check_and_repair_clean_timestamptz_df_preserves_timestamps():
    """open_time comes from the DB as tz-aware datetime (TIMESTAMPTZ schema).
    A gap-free frame needs no repair, and check_and_repair must neither raise
    nor corrupt the timestamps."""
    idx = pd.date_range("2024-01-01 00:00", periods=10, freq="1min", tz="UTC")
    df = _make_sql_df(idx)

    out = DataRepair(client_cls=None).check_and_repair(df.copy(), "BTCUSDT", "1m")

    # clean path returns the frame indexed by open_time
    assert list(out.index) == list(idx), "open_time timestamps were corrupted by repair"


# ---------------------------------------------------------------- pure helpers

def test_get_add_delete_sets_detects_single_gap():
    idx = pd.date_range("2024-01-01 00:00", periods=10, freq="1min", tz="UTC")
    missing = idx[5]
    gapped = idx.delete(5)
    df = pd.DataFrame(index=gapped)

    to_add, to_delete = DataRepair.get_add_delete_sets(df, frequency="1min")

    assert missing in to_add
    assert len(to_add) == 1
    assert to_delete == set()


def test_repair_index_returns_dataframe_then_two_sets():
    """Contract: repair_index returns (DataFrame, to_add_set, to_delete_set).
    Callers must unpack it (the gap-repair call site historically did not)."""
    idx = pd.date_range("2024-01-01 00:00", periods=5, freq="1min", tz="UTC")
    df = _make_sql_df(idx).set_index("open_time").drop(columns=["close_time"])
    to_add = {idx[2] + pd.Timedelta(seconds=30)}  # a stray extra label to add

    result = DataRepair.repair_index(to_add, set(), df.copy())

    assert isinstance(result, tuple) and len(result) == 3
    repaired_df, ret_add, ret_delete = result
    assert isinstance(repaired_df, pd.DataFrame)


class _FakeClient:
    """Returns canned Binance klines for any window (no network)."""
    def __init__(self, klines):
        self._klines = klines

    def get_historical_klines(self, symbol_pair, timeframe, start, end, limit=1000):
        return self._klines


def _binance_kline(ts: pd.Timestamp):
    ms = ts.value // 10 ** 6  # ns -> ms
    return [ms, 1.0, 2.0, 0.5, 1.5, 10.0, ms + 59_999, 0.0, 0, 0.0, 0.0, 0.0]


def test_check_and_repair_fills_gap_when_klines_partial():
    """#3: a 3-minute gap where Binance returns only the two boundary minutes
    forces the inner double-check (to_add_set non-empty) -> the repair_index
    call site at datafetcher.py:585. That site mis-uses the 3-tuple return as a
    DataFrame, so the gap repair currently breaks."""
    idx = pd.date_range("2024-01-01 00:00", periods=10, freq="1min", tz="UTC")
    df = _make_sql_df(idx)
    # drop minutes 4,5,6 -> 3-minute gap
    gapped = df.drop(index=[4, 5, 6]).reset_index(drop=True)

    # client returns klines only for minute 4 and minute 6 (minute 5 still missing)
    client = _FakeClient([_binance_kline(idx[4]), _binance_kline(idx[6])])

    out = DataRepair(client_cls=client).check_and_repair(gapped, "BTCUSDT", "1m")

    # after repair every minute in the original range must be present, with no gaps left
    assert len(out) == 10, f"expected 10 rows after gap repair, got {len(out)}"
    assert not out.isnull().values.any(), "gap was not fully filled"

    # open_time must survive the round-trip (after_preparation emits Unix-ms ints)
    got = pd.to_datetime(out["open_time"], unit="ms", utc=True)
    assert list(got) == list(idx), f"open_time corrupted: {got.iloc[0]} .. {got.iloc[-1]}"

    # the two boundary minutes must carry the REAL re-fetched klines (volume=10),
    # not interpolated values — that is the whole point of the re-fetch path
    volume_by_minute = dict(zip(got, out["volume"]))
    assert volume_by_minute[idx[4]] == 10.0
    assert volume_by_minute[idx[6]] == 10.0


def test_get_periods_groups_consecutive_then_breaks_on_gap():
    base = pd.Timestamp("2024-01-01 00:00", tz="UTC")
    delta = pd.to_timedelta(1, unit="m")
    # two consecutive, then a gap, then one
    points = [base, base + delta, base + 5 * delta]

    windows = DataRepair.get_periods(points, delta)

    assert windows[0] == (points[0], points[1], 2)
    assert windows[1] == (points[2], points[2], 1)
