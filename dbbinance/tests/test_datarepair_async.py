"""
DB-free тесты для DataRepair из asyncdatafetcher.py.

Async-поток (`AsyncDataUpdater.get_all_data_as_df`) отдаёт данные по `newsql_cols`
— БЕЗ колонки `id` (12 колонок, open_time/close_time как datetime64[ns, UTC]).
Поэтому фикстуры строятся без `id`, и проверяется именно этот путь
(`before_preparation`/`after_preparation` ветки "без id").

`DataRepair.check_and_repair` синхронный (дозагрузка идёт через синхронный
binance Client), поэтому тесты обычные, без asyncio. Binance не нужен — клиент фейковый.
"""
import numpy as np
import pandas as pd
import pytest

from dbbinance.fetcher.asyncdatafetcher import DataRepair

NEWSQL = ["open_time", "open", "high", "low", "close", "volume", "close_time",
          "quote_asset_volume", "trades", "taker_buy_base", "taker_buy_quote", "ignored"]
OHLCV = ["open", "high", "low", "close", "volume"]


def _make_newsql_df(index) -> pd.DataFrame:
    """Как AsyncDataFetcher.get_all_data_as_df: newsql_cols, без id,
    open_time/close_time как tz-aware UTC."""
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


class _FakeClient:
    def __init__(self, klines):
        self._klines = klines

    def get_historical_klines(self, symbol_pair, timeframe, start, end, limit=1000):
        return self._klines


def _binance_kline(ts: pd.Timestamp):
    ms = ts.value // 10 ** 6
    return [ms, 1.0, 2.0, 0.5, 1.5, 10.0, ms + 59_999, 0.0, 0, 0.0, 0.0, 0.0]


def test_async_repair_clean_df_preserves_timestamps():
    idx = pd.date_range("2024-01-01 00:00", periods=10, freq="1min", tz="UTC")
    df = _make_newsql_df(idx)

    out = DataRepair(client_cls=None).check_and_repair(df.copy(), "BTCUSDT", "1m")

    assert list(out.index) == list(idx), "open_time повреждён на чистых данных"


def test_async_repair_fills_gap_when_klines_partial():
    idx = pd.date_range("2024-01-01 00:00", periods=10, freq="1min", tz="UTC")
    df = _make_newsql_df(idx)
    gapped = df.drop(index=[4, 5, 6]).reset_index(drop=True)

    client = _FakeClient([_binance_kline(idx[4]), _binance_kline(idx[6])])
    out = DataRepair(client_cls=client).check_and_repair(gapped, "BTCUSDT", "1m")

    assert len(out) == 10, f"ожидалось 10 строк, получено {len(out)}"
    assert not out.isnull().values.any(), "пропуск не заполнен полностью"

    got = pd.to_datetime(out["open_time"], unit="ms", utc=True)
    assert list(got) == list(idx), f"open_time повреждён: {got.iloc[0]} .. {got.iloc[-1]}"

    volume_by_minute = dict(zip(got, out["volume"]))
    assert volume_by_minute[idx[4]] == 10.0
    assert volume_by_minute[idx[6]] == 10.0
