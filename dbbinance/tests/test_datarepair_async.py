"""
DB-free тесты для DataRepair из asyncdatafetcher.py.

Async-поток (`AsyncDataUpdater.get_all_data_as_df`) отдаёт данные по `newsql_cols`
— БЕЗ колонки `id` (12 колонок, open_time/close_time как datetime64[ns, UTC]).
Поэтому фикстуры строятся без `id`, и проверяется именно этот путь
(`before_preparation`/`after_preparation` ветки "без id").

Покрытие зеркалит синхронный двойник (test_datarepair_sync.py): чистый кадр,
single-gap (full/partial/empty), multi-gap, контракт колонок для
insert_klines_to_table и guard на отсутствие 'id'.

`DataRepair.check_and_repair` синхронный (дозагрузка идёт через синхронный
binance Client), поэтому тесты обычные, без asyncio. Binance не нужен — клиент фейковый.
"""
import numpy as np
import pandas as pd
import pytest

from dbbinance.fetcher import Constants
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


def _binance_kline(ts: pd.Timestamp, volume: float = 10.0):
    ms = ts.value // 10 ** 6
    return [ms, 1.0, 2.0, 0.5, 1.5, volume, ms + 59_999, 0.0, 0, 0.0, 0.0, 0.0]


class _FakeClient:
    """Фикс-список: возвращает одни и те же свечи на любой запрос (single-gap)."""
    def __init__(self, klines):
        self._klines = klines

    def get_historical_klines(self, symbol_pair, timeframe, start, end, limit=1000):
        return self._klines


class _WindowFakeClient:
    """Окно-зависимый фейк: генерирует свечи по запрошенному окну [start, end],
    чтобы каждый из нескольких разрозненных гэпов получил свои выровненные данные.
    mode: 'full' / 'partial' / 'empty'."""
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


def _assert_repair_output_contract(out, idx):
    """Repair-ветка прогоняет after_preparation + dtype-loop. Её выход — ровно то,
    что потребляет insert_klines_to_table (data_df.values.tolist()):
    RangeIndex, columns == newsql_cols (без 'id'), open_time/close_time как Unix-ms int64,
    каждая строка — 12 полей."""
    assert list(out.columns) == list(Constants.newsql_cols), \
        f"порядок/набор колонок разошёлся с newsql_cols: {list(out.columns)}"
    assert "id" not in out.columns
    assert out["open_time"].dtype == "int64", "open_time должен быть Unix-ms int64 после after_preparation"
    assert out["close_time"].dtype == "int64", "close_time должен быть Unix-ms int64 после after_preparation"
    assert len(out) == len(idx)
    got = pd.to_datetime(out["open_time"], unit="ms", utc=True)
    assert list(got) == list(idx), f"open_time повреждён: {got.iloc[0]} .. {got.iloc[-1]}"
    for row in out.values.tolist():
        assert len(row) == 12, f"ожидалось 12 полей на строку для insert_klines_to_table, получено {len(row)}"


# ---------------------------------------------------------------- clean path

def test_async_repair_clean_df_preserves_timestamps():
    idx = pd.date_range("2024-01-01 00:00", periods=10, freq="1min", tz="UTC")
    df = _make_newsql_df(idx)

    out = DataRepair(client_cls=None).check_and_repair(df.copy(), "BTCUSDT", "1m")

    assert list(out.index) == list(idx), "open_time повреждён на чистых данных"
    assert "id" not in out.columns, "колонка 'id' не должна появляться в no-id пути"


# ---------------------------------------------------------------- gap repair

def test_async_repair_fills_gap_when_klines_partial():
    """3-минутный гэп; Binance отдаёт только две граничные минуты (4 и 6).
    Граничные минуты несут РЕАЛЬНЫЕ свечи (volume=10), минута 5 — rolling-mean."""
    idx = pd.date_range("2024-01-01 00:00", periods=10, freq="1min", tz="UTC")
    df = _make_newsql_df(idx)
    gapped = df.drop(index=[4, 5, 6]).reset_index(drop=True)

    client = _FakeClient([_binance_kline(idx[4]), _binance_kline(idx[6])])
    out = DataRepair(client_cls=client).check_and_repair(gapped, "BTCUSDT", "1m")

    assert not out.isnull().values.any(), "пропуск не заполнен полностью"
    _assert_repair_output_contract(out, idx)

    volume_by_minute = dict(zip(pd.to_datetime(out["open_time"], unit="ms", utc=True), out["volume"]))
    assert volume_by_minute[idx[4]] == 10.0
    assert volume_by_minute[idx[6]] == 10.0


def test_async_repair_fills_gap_when_klines_full():
    idx = pd.date_range("2024-01-01 00:00", periods=10, freq="1min", tz="UTC")
    df = _make_newsql_df(idx)
    gapped = df.drop(index=[4, 5, 6]).reset_index(drop=True)

    client = _WindowFakeClient(mode="full")
    out = DataRepair(client_cls=client).check_and_repair(gapped, "BTCUSDT", "1m")

    assert not out.isnull().values.any(), "пропуск не заполнен полностью"
    _assert_repair_output_contract(out, idx)

    volume_by_minute = dict(zip(pd.to_datetime(out["open_time"], unit="ms", utc=True), out["volume"]))
    for minute in (idx[4], idx[5], idx[6]):
        assert volume_by_minute[minute] == 10.0, f"минута гэпа {minute} должна нести РЕАЛЬНУЮ свечу"


def test_async_repair_fills_gap_when_klines_empty():
    """Binance ничего не отдаёт — rolling-mean fallback, без проверки реальных значений."""
    idx = pd.date_range("2024-01-01 00:00", periods=10, freq="1min", tz="UTC")
    df = _make_newsql_df(idx)
    gapped = df.drop(index=[4, 5, 6]).reset_index(drop=True)

    client = _WindowFakeClient(mode="empty")
    out = DataRepair(client_cls=client).check_and_repair(gapped, "BTCUSDT", "1m")

    assert not out.isnull().values.any(), "rolling-mean fallback оставил пропуски"
    _assert_repair_output_contract(out, idx)


def test_async_repair_fills_multiple_gaps():
    """Два разрозненных гэпа (минуты 3,4 и минута 8); окно-зависимый фейк отдаёт
    реальные данные на каждое окно независимо."""
    idx = pd.date_range("2024-01-01 00:00", periods=15, freq="1min", tz="UTC")
    df = _make_newsql_df(idx)
    gapped = df.drop(index=[3, 4, 8]).reset_index(drop=True)

    client = _WindowFakeClient(mode="full")
    out = DataRepair(client_cls=client).check_and_repair(gapped, "BTCUSDT", "1m")

    assert not out.isnull().values.any(), "multi-gap repair оставил пропуски"
    _assert_repair_output_contract(out, idx)

    volume_by_minute = dict(zip(pd.to_datetime(out["open_time"], unit="ms", utc=True), out["volume"]))
    for minute in (idx[3], idx[4], idx[8]):
        assert volume_by_minute[minute] == 10.0, f"минута гэпа {minute} должна нести РЕАЛЬНУЮ свечу"


class _MixedWindowFakeClient:
    """Per-window fake: РЕАЛЬНЫЕ свечи для ПЕРВОГО запрошенного окна и ничего для
    остальных. Зеркалит синхронный двойник: один гэп получает реальные данные,
    другой — rolling-mean fallback."""
    def __init__(self, freq="1min"):
        self.freq = freq
        self._calls = 0

    def get_historical_klines(self, symbol_pair, timeframe, start, end, limit=1000):
        self._calls += 1
        if self._calls > 1:
            return []
        rng = pd.date_range(pd.Timestamp(start, tz="UTC"), pd.Timestamp(end, tz="UTC"), freq=self.freq)
        # отличительный sentinel volume, который синтетический кадр (volume = minute+1,
        # диапазон 1..15) не может произвести через rolling-mean — реальную свечу
        # однозначно видно на фоне fallback-строки.
        return [_binance_kline(ts, volume=777.0) for ts in rng]


def test_async_repair_multi_gap_mixed_real_and_rolling():
    """Два разрозненных гэпа; первое окно получает РЕАЛЬНЫЕ свечи, второе — ничего
    (rolling-mean fallback). Оба окна полностью заполнены, контракт держится,
    реальное окно несёт sentinel volume, а fallback-окно — нет."""
    idx = pd.date_range("2024-01-01 00:00", periods=15, freq="1min", tz="UTC")
    df = _make_newsql_df(idx)
    gapped = df.drop(index=[3, 4, 10]).reset_index(drop=True)

    client = _MixedWindowFakeClient()
    out = DataRepair(client_cls=client).check_and_repair(gapped, "BTCUSDT", "1m")

    assert not out.isnull().values.any(), "mixed multi-gap repair оставил пропуски"
    _assert_repair_output_contract(out, idx)

    volume_by_minute = dict(zip(pd.to_datetime(out["open_time"], unit="ms", utc=True), out["volume"]))
    assert volume_by_minute[idx[3]] == 777.0, "первое окно должно нести РЕАЛЬНУЮ свечу"
    assert volume_by_minute[idx[4]] == 777.0, "первое окно должно нести РЕАЛЬНУЮ свечу"
    assert volume_by_minute[idx[10]] != 777.0, \
        "второе окно заполнено rolling-mean, а не sentinel реальной свечи"


# ---------------------------------------------------------------- pure helpers

def test_async_get_add_delete_sets_detects_single_gap():
    """Парность с sync-двойником: классовый хелпер async-копии DataRepair."""
    idx = pd.date_range("2024-01-01 00:00", periods=10, freq="1min", tz="UTC")
    missing = idx[5]
    gapped = idx.delete(5)
    df = pd.DataFrame(index=gapped)

    to_add, to_delete = DataRepair.get_add_delete_sets(df, frequency="1min")

    assert missing in to_add
    assert len(to_add) == 1
    assert to_delete == set()


def test_async_repair_index_returns_dataframe_then_two_sets():
    """Контракт: repair_index возвращает (DataFrame, to_add_set, to_delete_set)."""
    idx = pd.date_range("2024-01-01 00:00", periods=5, freq="1min", tz="UTC")
    df = _make_newsql_df(idx).set_index("open_time").drop(columns=["close_time"])
    to_add = {idx[2] + pd.Timedelta(seconds=30)}

    result = DataRepair.repair_index(to_add, set(), df.copy())

    assert isinstance(result, tuple) and len(result) == 3
    repaired_df, ret_add, ret_delete = result
    assert isinstance(repaired_df, pd.DataFrame)


# ---------------------------------------------------------------- guards

def test_async_constants_ohlcv_column_set_has_no_id():
    """Канонический набор колонок записи OHLCV — newsql_cols: 12 колонок, без 'id'.
    Зеркалит sync-гард: легаси id-несущие константы должны отсутствовать."""
    assert "id" not in Constants.newsql_cols
    assert len(Constants.newsql_cols) == 12
    assert list(Constants.newsql_cols) == NEWSQL
    assert not hasattr(Constants, "sql_cols"), "легаси Constants.sql_cols должен быть удалён"
    assert not hasattr(Constants, "sql_dtypes"), "легаси Constants.sql_dtypes должен быть удалён"


def test_async_no_residual_id_column_anywhere_in_repair():
    idx = pd.date_range("2024-01-01 00:00", periods=8, freq="1min", tz="UTC")
    df = _make_newsql_df(idx)

    clean = DataRepair(client_cls=None).check_and_repair(df.copy(), "BTCUSDT", "1m")
    assert "id" not in clean.columns and "id" not in clean.index.names

    gapped = df.drop(index=[3]).reset_index(drop=True)
    repaired = DataRepair(client_cls=_WindowFakeClient(mode="full")).check_and_repair(
        gapped, "BTCUSDT", "1m")
    assert "id" not in repaired.columns
