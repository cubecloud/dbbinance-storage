"""
DB-free end-to-end contract test: DataRepair.check_and_repair output ->
the REAL PostgreSQLDatabase.insert_klines_to_table (no DB, no network).

This pins the positional column contract that the repair/read path relies on.
insert_klines_to_table consumes `data_df.values.tolist()` rows positionally
(k[0]=open_time ms, k[6]=close_time ms, k[8]=trades int, 12 fields total) and
ON CONFLICT (open_time). If after_preparation ever drifts (e.g. a resurrected
'id' column shifting close_time off position 6), these assertions break.

We deliberately call the PRODUCTION inserter rather than reimplementing its
k[0..11] comprehension: a hand-copied row-builder would pass by construction and
never catch drift between the test's copy and the real code. We bypass the
DB-connecting __init__ via __new__ and monkeypatch the module-level ThreadPool
so cur.executemany simply captures the rows it would have sent to PostgreSQL.
"""
import datetime
from datetime import timezone

import numpy as np
import pandas as pd
import pytest

from dbbinance.fetcher import Constants
from dbbinance.fetcher import datafetcher as sync_mod
from dbbinance.fetcher.datafetcher import DataRepair, PostgreSQLDatabase

NEWSQL = ["open_time", "open", "high", "low", "close", "volume", "close_time",
          "quote_asset_volume", "trades", "taker_buy_base", "taker_buy_quote", "ignored"]


def _make_newsql_df(index) -> pd.DataFrame:
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
        "trades": np.arange(n, dtype="int64") + 100,
        "taker_buy_base": np.zeros(n, dtype=float),
        "taker_buy_quote": np.zeros(n, dtype=float),
        "ignored": np.zeros(n, dtype=float),
    })
    df["open_time"] = df["open_time"].astype("datetime64[ns, UTC]")
    df["close_time"] = df["close_time"].astype("datetime64[ns, UTC]")
    return df


def _binance_kline(ts: pd.Timestamp, volume: float = 10.0):
    ms = ts.value // 10 ** 6
    return [ms, 1.0, 2.0, 0.5, 1.5, volume, ms + 59_999, 0.0, 0, 0.0, 0.0, 0.0]


class _WindowFakeClient:
    def __init__(self, mode="full", freq="1min"):
        self.mode = mode
        self.freq = freq

    def get_historical_klines(self, symbol_pair, timeframe, start, end, limit=1000):
        rng = pd.date_range(pd.Timestamp(start, tz="UTC"), pd.Timestamp(end, tz="UTC"), freq=self.freq)
        if self.mode == "empty" or len(rng) == 0:
            return []
        return [_binance_kline(ts) for ts in rng]


# --- fake DB plumbing: capture what executemany would have sent to PostgreSQL ---

class _FakeCursor:
    def __init__(self, sink):
        self._sink = sink

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def executemany(self, query, rows):
        self._sink["query"] = query
        self._sink["rows"] = list(rows)


class _FakeConn:
    def __init__(self, sink):
        self._sink = sink
        self.committed = False

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def cursor(self):
        return _FakeCursor(self._sink)

    def commit(self):
        self.committed = True
        self._sink["committed"] = True


def _make_capturing_db(monkeypatch, sink):
    """Real PostgreSQLDatabase instance with __init__ bypassed and ThreadPool
    monkeypatched so insert_klines_to_table runs without a live connection."""
    inst = PostgreSQLDatabase.__new__(PostgreSQLDatabase)
    inst.pool = object()  # dummy; ThreadPool is faked, never dereferenced
    # SQLMeta.__del__ does `del self.db_mgr`; supply a placeholder so GC of this
    # __new__-built instance doesn't emit a spurious AttributeError warning.
    inst.db_mgr = None

    def _fake_threadpool(_pool):
        return _FakeConn(sink)

    monkeypatch.setattr(sync_mod, "ThreadPool", _fake_threadpool)
    return inst


def _repair_output(client_mode="full", drop=(4, 5, 6)):
    idx = pd.date_range("2024-01-01 00:00", periods=10, freq="1min", tz="UTC")
    df = _make_newsql_df(idx)
    gapped = df.drop(index=list(drop)).reset_index(drop=True)
    out = DataRepair(client_cls=_WindowFakeClient(mode=client_mode)).check_and_repair(
        gapped, "BTCUSDT", "1m")
    return idx, out


def test_repair_output_roundtrips_through_real_insert_klines(monkeypatch):
    """Feed the REAL repair output (values.tolist()) to the REAL inserter and
    assert the positional mapping it builds is correct, no DB touched."""
    idx, out = _repair_output(client_mode="full")
    # precondition: repair output is exactly the newsql contract
    assert list(out.columns) == list(Constants.newsql_cols)
    assert "id" not in out.columns

    sink = {}
    db = _make_capturing_db(monkeypatch, sink)
    db.insert_klines_to_table("spot_data_btcusdt_1m", out.values.tolist())

    rows = sink["rows"]
    assert len(rows) == len(idx), "inserter dropped/added rows vs repair output"

    first = rows[0]
    assert len(first) == 12, "inserter must build 12-field tuples (newsql, no id)"
    # k[0] -> open_time as tz-aware UTC datetime, aligned to the real minute grid
    assert isinstance(first[0], datetime.datetime) and first[0].tzinfo is not None
    assert first[0] == idx[0].to_pydatetime(), "open_time landed at the wrong minute"
    # k[6] -> close_time datetime (position 6, between volume and quote_asset_volume)
    assert isinstance(first[6], datetime.datetime) and first[6].tzinfo is not None
    # k[8] -> trades coerced to int
    assert isinstance(first[8], int)
    # every open_time in order matches the full minute grid (gap re-fetched & spliced)
    open_times = [r[0] for r in rows]
    assert open_times == [m.to_pydatetime() for m in idx], \
        "open_times not contiguous on the real grid after repair+insert"
    assert sink.get("committed") is True, "insert must commit the transaction"


def test_repair_output_insert_contract_with_empty_klines_fallback(monkeypatch):
    """Even when Binance returns nothing (rolling-mean fallback), the frame still
    round-trips through the real inserter with the correct positional contract."""
    idx, out = _repair_output(client_mode="empty")
    sink = {}
    db = _make_capturing_db(monkeypatch, sink)
    db.insert_klines_to_table("spot_data_btcusdt_1m", out.values.tolist())

    rows = sink["rows"]
    assert len(rows) == len(idx)
    assert [r[0] for r in rows] == [m.to_pydatetime() for m in idx]
    assert all(len(r) == 12 for r in rows)
    assert all(isinstance(r[8], int) for r in rows), "trades must coerce to int for every row"


def test_insert_query_targets_open_time_conflict_no_id(monkeypatch):
    """The production INSERT lists exactly the 12 newsql columns and uses
    ON CONFLICT (open_time) — no 'id' column anywhere in the write path."""
    idx, out = _repair_output(client_mode="full")
    sink = {}
    db = _make_capturing_db(monkeypatch, sink)
    db.insert_klines_to_table("spot_data_btcusdt_1m", out.values.tolist())

    # The query is a psycopg2 sql.Composed; rendering as_string() needs a live
    # connection only because of the table Identifier. The column list and the
    # ON CONFLICT clause live in the fixed sql.SQL parts (each exposes .string),
    # so we concatenate those literal fragments — no connection required.
    composed = sink["query"]
    parts = getattr(composed, "seq", None) or getattr(composed, "_wrapped", [composed])
    query_text = "".join(getattr(p, "string", "") for p in parts)
    assert "ON CONFLICT (open_time)" in query_text
    # the 'id' column must not be part of the INSERT column list
    assert " id," not in query_text and "(id" not in query_text
    for col in Constants.newsql_cols:
        assert col in query_text, f"newsql column {col} missing from INSERT"
