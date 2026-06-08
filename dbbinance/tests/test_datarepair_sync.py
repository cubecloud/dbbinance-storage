"""
DB-free tests for the synchronous DataRepair (datafetcher.py).

No PostgreSQL and no Binance API are required: the input DataFrames are built
to match exactly what `get_all_data_as_df` returns on the migrated TIMESTAMPTZ
schema (newsql_cols, NO 'id'), and the Binance client is a local fake.

These pin:
  - the TIMESTAMPTZ-schema behaviour (open_time arrives as tz-aware datetime);
  - the no-'id' before/after_preparation round-trip;
  - the repair_index 3-tuple contract;
  - the gap re-fetch splice (full / partial / empty klines, single & multi gap)
    landing REAL re-fetched values at the correct open_times;
  - the output column contract that insert_klines_to_table consumes
    (12 cols == newsql_cols, open_time/close_time as Unix-ms int64).
"""
import numpy as np
import pandas as pd
import pytest

from dbbinance.fetcher import Constants
from dbbinance.fetcher.datafetcher import DataRepair

NEWSQL = ["open_time", "open", "high", "low", "close", "volume", "close_time",
          "quote_asset_volume", "trades", "taker_buy_base", "taker_buy_quote", "ignored"]
OHLCV = ["open", "high", "low", "close", "volume"]


def _make_newsql_df(index) -> pd.DataFrame:
    """Build a DataFrame shaped like the migrated get_all_data_as_df output:
    newsql_cols (NO 'id'), open_time/close_time as tz-aware UTC. This matches the
    real TIMESTAMPTZ tables created by create_table (open_time PRIMARY KEY, no id)."""
    n = len(index)
    df = pd.DataFrame({
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


# Backwards-compatible alias: the old fixture injected an 'id' column for the
# legacy BIGINT schema. The migrated schema has no 'id', so the two are now
# identical — keep the name so historical references stay valid.
_make_sql_df = _make_newsql_df


def _binance_kline(ts: pd.Timestamp, volume: float = 10.0):
    """One Binance kline row (12 fields) for the given open_time.
    volume is distinctive (default 10.0) so re-fetched REAL rows are
    distinguishable from the synthetic in-frame rows (volume = minute+1)."""
    ms = ts.value // 10 ** 6  # ns -> ms
    return [ms, 1.0, 2.0, 0.5, 1.5, volume, ms + 59_999, 0.0, 0, 0.0, 0.0, 0.0]


class _FakeClient:
    """Fixed-list fake: returns the same canned klines for every window.
    Useful for the single-gap tests where there is one re-fetch call."""
    def __init__(self, klines):
        self._klines = klines

    def get_historical_klines(self, symbol_pair, timeframe, start, end, limit=1000):
        return self._klines


class _WindowFakeClient:
    """Window-aware fake: generates klines for the requested [start, end] minute
    window, so multiple disjoint gaps each get correct, aligned re-fetched data.

    mode:
      "full"    -> return a kline for every minute in [start, end]
      "partial" -> return only the two boundary minutes (interior stays missing)
      "empty"   -> return [] (forces the rolling-mean fallback path)
    """
    def __init__(self, mode="full", freq="1min"):
        self.mode = mode
        self.freq = freq

    def get_historical_klines(self, symbol_pair, timeframe, start, end, limit=1000):
        start_ts = pd.Timestamp(start, tz="UTC")
        end_ts = pd.Timestamp(end, tz="UTC")
        rng = pd.date_range(start_ts, end_ts, freq=self.freq)
        if self.mode == "empty" or len(rng) == 0:
            return []
        if self.mode == "partial" and len(rng) >= 2:
            picks = [rng[0], rng[-1]]
        else:
            picks = list(rng)
        return [_binance_kline(ts) for ts in picks]


# ---------------------------------------------------------------- clean path

def test_check_and_repair_clean_newsql_df_preserves_timestamps():
    """Real migrated schema: open_time arrives tz-aware, frame has NO 'id'.
    A gap-free frame must neither raise (KeyError 'id') nor corrupt timestamps.
    Clean path returns the before_preparation frame (indexed by open_time)."""
    idx = pd.date_range("2024-01-01 00:00", periods=10, freq="1min", tz="UTC")
    df = _make_newsql_df(idx)

    out = DataRepair(client_cls=None).check_and_repair(df.copy(), "BTCUSDT", "1m")

    assert list(out.index) == list(idx), "open_time timestamps were corrupted by repair"
    assert "id" not in out.columns, "no 'id' column may appear in the no-id repair path"


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
    df = _make_newsql_df(idx).set_index("open_time").drop(columns=["close_time"])
    to_add = {idx[2] + pd.Timedelta(seconds=30)}  # a stray extra label to add

    result = DataRepair.repair_index(to_add, set(), df.copy())

    assert isinstance(result, tuple) and len(result) == 3
    repaired_df, ret_add, ret_delete = result
    assert isinstance(repaired_df, pd.DataFrame)


def test_get_periods_groups_consecutive_then_breaks_on_gap():
    base = pd.Timestamp("2024-01-01 00:00", tz="UTC")
    delta = pd.to_timedelta(1, unit="m")
    # two consecutive, then a gap, then one
    points = [base, base + delta, base + 5 * delta]

    windows = DataRepair.get_periods(points, delta)

    assert windows[0] == (points[0], points[1], 2)
    assert windows[1] == (points[2], points[2], 1)


# ---------------------------------------------------------------- gap repair

def _assert_repair_output_contract(out, idx):
    """The repair branch runs after_preparation + the dtype loop. Its output is
    exactly what insert_klines_to_table consumes:
      - RangeIndex (open_time is a column, not the index)
      - columns == newsql_cols, in that order, no 'id'
      - open_time / close_time are Unix-ms int64
      - every row is a 12-element list in newsql order (values.tolist() contract)
    """
    assert list(out.columns) == list(Constants.newsql_cols), \
        f"column order/set drifted from newsql_cols: {list(out.columns)}"
    assert "id" not in out.columns
    assert out["open_time"].dtype == "int64", "open_time must be Unix-ms int64 after after_preparation"
    assert out["close_time"].dtype == "int64", "close_time must be Unix-ms int64 after after_preparation"
    assert len(out) == len(idx)
    # round-trip: open_time int -> tz-aware UTC must equal the original minutes
    got = pd.to_datetime(out["open_time"], unit="ms", utc=True)
    assert list(got) == list(idx), f"open_time corrupted: {got.iloc[0]} .. {got.iloc[-1]}"
    # values.tolist() rows the inserter consumes: 12 fields each
    for row in out.values.tolist():
        assert len(row) == 12, f"expected 12-field rows for insert_klines_to_table, got {len(row)}"


def test_check_and_repair_fills_gap_when_klines_partial():
    """3-minute gap (minutes 4,5,6); Binance returns only the two boundary
    minutes (4 and 6). Forces the inner double-check and the repair_index splice.
    The two boundary minutes must carry REAL re-fetched klines (volume=10),
    minute 5 (not returned) is rolling-mean filled — assert no gaps remain."""
    idx = pd.date_range("2024-01-01 00:00", periods=10, freq="1min", tz="UTC")
    df = _make_newsql_df(idx)
    gapped = df.drop(index=[4, 5, 6]).reset_index(drop=True)

    client = _FakeClient([_binance_kline(idx[4]), _binance_kline(idx[6])])
    out = DataRepair(client_cls=client).check_and_repair(gapped, "BTCUSDT", "1m")

    assert not out.isnull().values.any(), "gap was not fully filled"
    _assert_repair_output_contract(out, idx)

    volume_by_minute = dict(zip(pd.to_datetime(out["open_time"], unit="ms", utc=True), out["volume"]))
    assert volume_by_minute[idx[4]] == 10.0, "boundary minute 4 must carry the REAL re-fetched kline"
    assert volume_by_minute[idx[6]] == 10.0, "boundary minute 6 must carry the REAL re-fetched kline"


def test_check_and_repair_fills_gap_when_klines_full():
    """3-minute gap; Binance returns ALL three missing minutes. Every gap minute
    must carry the REAL re-fetched value (volume=10), not a rolling-mean guess."""
    idx = pd.date_range("2024-01-01 00:00", periods=10, freq="1min", tz="UTC")
    df = _make_newsql_df(idx)
    gapped = df.drop(index=[4, 5, 6]).reset_index(drop=True)

    client = _WindowFakeClient(mode="full")
    out = DataRepair(client_cls=client).check_and_repair(gapped, "BTCUSDT", "1m")

    assert not out.isnull().values.any(), "gap was not fully filled"
    _assert_repair_output_contract(out, idx)

    volume_by_minute = dict(zip(pd.to_datetime(out["open_time"], unit="ms", utc=True), out["volume"]))
    for minute in (idx[4], idx[5], idx[6]):
        assert volume_by_minute[minute] == 10.0, f"gap minute {minute} must carry the REAL re-fetched kline"


def test_check_and_repair_fills_gap_when_klines_empty():
    """3-minute gap; Binance returns nothing (empty klines). The rolling-mean
    fallback must still produce a complete, contract-shaped frame with no nulls.
    We do NOT assert real values here (none were available)."""
    idx = pd.date_range("2024-01-01 00:00", periods=10, freq="1min", tz="UTC")
    df = _make_newsql_df(idx)
    gapped = df.drop(index=[4, 5, 6]).reset_index(drop=True)

    client = _WindowFakeClient(mode="empty")
    out = DataRepair(client_cls=client).check_and_repair(gapped, "BTCUSDT", "1m")

    assert not out.isnull().values.any(), "rolling-mean fallback left nulls"
    _assert_repair_output_contract(out, idx)


def test_check_and_repair_fills_multiple_gaps():
    """Two disjoint gaps in one frame (minutes 3,4 and minute 8). A window-aware
    fake client returns real klines for each requested window independently, so
    every missing minute is filled with REAL data and the contract holds."""
    idx = pd.date_range("2024-01-01 00:00", periods=15, freq="1min", tz="UTC")
    df = _make_newsql_df(idx)
    gapped = df.drop(index=[3, 4, 8]).reset_index(drop=True)

    client = _WindowFakeClient(mode="full")
    out = DataRepair(client_cls=client).check_and_repair(gapped, "BTCUSDT", "1m")

    assert not out.isnull().values.any(), "multi-gap repair left nulls"
    _assert_repair_output_contract(out, idx)

    volume_by_minute = dict(zip(pd.to_datetime(out["open_time"], unit="ms", utc=True), out["volume"]))
    for minute in (idx[3], idx[4], idx[8]):
        assert volume_by_minute[minute] == 10.0, f"gap minute {minute} must carry the REAL re-fetched kline"


class _MixedWindowFakeClient:
    """Per-window fake: returns REAL klines for the FIRST requested window and
    nothing for the rest. Exercises the per-window iteration distinctly — one
    gap gets real re-fetched data, the other falls back to rolling-mean."""
    def __init__(self, freq="1min"):
        self.freq = freq
        self._calls = 0

    def get_historical_klines(self, symbol_pair, timeframe, start, end, limit=1000):
        self._calls += 1
        if self._calls > 1:
            return []
        rng = pd.date_range(pd.Timestamp(start, tz="UTC"), pd.Timestamp(end, tz="UTC"), freq=self.freq)
        # distinctive sentinel volume that the synthetic frame (volume = minute+1,
        # range 1..15) can never produce via rolling-mean, so a real re-fetched row
        # is unambiguously distinguishable from a rolling-mean fallback row.
        return [_binance_kline(ts, volume=777.0) for ts in rng]


def test_check_and_repair_multi_gap_mixed_real_and_rolling():
    """Two disjoint gaps; the first window gets REAL klines, the second gets
    nothing (rolling-mean fallback). Both windows must be fully filled, the
    output contract holds, and the real window carries the sentinel volume while
    the fallback window does NOT (it is a rolling-mean guess, not real data)."""
    idx = pd.date_range("2024-01-01 00:00", periods=15, freq="1min", tz="UTC")
    df = _make_newsql_df(idx)
    gapped = df.drop(index=[3, 4, 10]).reset_index(drop=True)

    client = _MixedWindowFakeClient()
    out = DataRepair(client_cls=client).check_and_repair(gapped, "BTCUSDT", "1m")

    assert not out.isnull().values.any(), "mixed multi-gap repair left nulls"
    _assert_repair_output_contract(out, idx)

    volume_by_minute = dict(zip(pd.to_datetime(out["open_time"], unit="ms", utc=True), out["volume"]))
    # first window (minutes 3,4): REAL re-fetched klines (sentinel volume 777)
    assert volume_by_minute[idx[3]] == 777.0, "first-window gap must carry the REAL re-fetched kline"
    assert volume_by_minute[idx[4]] == 777.0, "first-window gap must carry the REAL re-fetched kline"
    # second window (minute 10): rolling-mean fallback, NOT the real-kline sentinel
    assert volume_by_minute[idx[10]] != 777.0, \
        "second-window gap was filled by rolling-mean, not the real-kline sentinel"


# ---------------------------------------------------------------- guards

def test_no_residual_id_column_anywhere_in_repair():
    """Defence-in-depth: neither the clean path nor the repair path may produce
    an 'id' column. The legacy BIGINT 'id' rudiment is fully gone."""
    idx = pd.date_range("2024-01-01 00:00", periods=8, freq="1min", tz="UTC")
    df = _make_newsql_df(idx)

    clean = DataRepair(client_cls=None).check_and_repair(df.copy(), "BTCUSDT", "1m")
    assert "id" not in clean.columns and "id" not in clean.index.names

    gapped = df.drop(index=[3]).reset_index(drop=True)
    repaired = DataRepair(client_cls=_WindowFakeClient(mode="full")).check_and_repair(
        gapped, "BTCUSDT", "1m")
    assert "id" not in repaired.columns


def test_constants_ohlcv_column_set_has_no_id():
    """The canonical OHLCV write column set is newsql_cols: 12 cols, no 'id'."""
    assert "id" not in Constants.newsql_cols
    assert len(Constants.newsql_cols) == 12
    assert list(Constants.newsql_cols) == NEWSQL
    # legacy id-bearing constants were removed from the source
    assert not hasattr(Constants, "sql_cols"), "legacy Constants.sql_cols should be gone"
    assert not hasattr(Constants, "sql_dtypes"), "legacy Constants.sql_dtypes should be gone"
