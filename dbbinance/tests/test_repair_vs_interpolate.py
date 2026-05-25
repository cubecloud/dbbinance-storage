"""
Сравнение восстановления пропусков (gaps) в 1-минутных OHLCV данных:

  1. DataRepair.check_and_repair — дозагружает РЕАЛЬНЫЕ свечи с Binance
     (fallback rolling().mean() только для того, что биржа не отдала).
  2. pandas interpolate — наивная линейная интерполяция по всем колонкам.
  3. pandas "разумная" интерполяция — linear по OHLC, ffill по volume
     (объёмы линейно интерполировать нельзя — это спайки).

Для каждого размера гэпа (10 / 30 / 100 таймфреймов) считаем ошибку каждого
метода относительно ИСТИНЫ (truth) по колонкам OHLCV и печатаем таблицу.

ВАЖНО (асимметрия, проговорена честно): repair берёт данные с того же Binance,
что и эталон, поэтому repair_error ≈ 0 ПО ПОСТРОЕНИЮ — не потому что "repair
лучше алгоритмически", а потому что он возвращает настоящие данные, а не выдумку.
Смысл сравнения — показать, насколько interpolate расходится с истиной и как
этот разрыв растёт с размером гэпа.

Тест ходит в сеть (Binance) и читается под `pytest -s`.
"""
import datetime
from datetime import timezone

import numpy as np
import pandas as pd
import pytest

from dbbinance.fetcher import Constants
from dbbinance.fetcher.datafetcher import DataRepair
from dbbinance.config.configbinance import ConfigBinance
from binance.client import Client

OHLCV = ["open", "high", "low", "close", "volume"]
PAD = 10                      # минут контекста с каждой стороны гэпа
BASE_START = "01 Jun 2023 00:00:00"   # стабильный исторический диапазон


@pytest.fixture(scope="module")
def client():
    return Client(api_key=ConfigBinance.BINANCE_API_KEY,
                  api_secret=ConfigBinance.BINANCE_API_SECRET)


def _klines_to_truth_df(klines) -> pd.DataFrame:
    """Свечи Binance -> DataFrame в схеме get_all_data_as_df (sql_cols,
    open_time/close_time как datetime64[ns, UTC])."""
    df = pd.DataFrame(klines, columns=list(Constants.binance_cols))
    df = df.iloc[:, :len(Constants.binance_cols)]
    for c in ["open", "high", "low", "close", "volume",
              "quote_asset_volume", "taker_buy_base", "taker_buy_quote", "ignored"]:
        df[c] = df[c].astype(float)
    df["trades"] = df["trades"].astype("int64")
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    df.insert(0, "id", np.arange(1, len(df) + 1, dtype="int64"))
    return df


def _fetch_truth(client, n_minutes: int) -> pd.DataFrame:
    start = datetime.datetime.strptime(BASE_START, "%d %b %Y %H:%M:%S").replace(tzinfo=timezone.utc)
    end = start + datetime.timedelta(minutes=n_minutes - 1)
    klines = client.get_historical_klines(
        "BTCUSDT", "1m",
        start.strftime("%d %b %Y %H:%M:%S"),
        end.strftime("%d %b %Y %H:%M:%S"),
        limit=1000,
    )
    df = _klines_to_truth_df(klines)
    return df.iloc[:n_minutes].reset_index(drop=True)


def _repair_reconstruct(gapped: pd.DataFrame, client) -> pd.DataFrame:
    out = DataRepair(client_cls=client).check_and_repair(gapped.copy(), "BTCUSDT", "1m")
    # после фиксов open_time возвращается как Unix-ms int -> назад в tz-aware UTC
    out = out.copy()
    out["open_time"] = pd.to_datetime(out["open_time"], unit="ms", utc=True)
    return out.set_index("open_time")


def _interp_reconstruct(gapped: pd.DataFrame, full_index: pd.DatetimeIndex):
    g = gapped.set_index("open_time").reindex(full_index)
    naive = g[OHLCV].interpolate(method="linear", limit_direction="both")
    smart = g[["open", "high", "low", "close"]].interpolate(method="linear", limit_direction="both")
    smart = smart.assign(volume=g["volume"].ffill())
    return naive, smart[OHLCV]


def _metrics(recon_gap: pd.DataFrame, truth_gap: pd.DataFrame) -> pd.DataFrame:
    rows = {}
    for col in OHLCV:
        diff = (recon_gap[col].to_numpy(float) - truth_gap[col].to_numpy(float))
        truth_scale = np.mean(np.abs(truth_gap[col].to_numpy(float))) or 1.0
        rows[col] = {
            "MAE": np.mean(np.abs(diff)),
            "MaxAE": np.max(np.abs(diff)),
            "rel%": 100.0 * np.mean(np.abs(diff)) / truth_scale,
        }
    return pd.DataFrame(rows).T


@pytest.mark.parametrize("gap", [10, 30, 100])
def test_repair_vs_interpolate(client, gap):
    truth = _fetch_truth(client, PAD + gap + PAD)
    truth_idx = truth.set_index("open_time").index
    gap_labels = truth_idx[PAD:PAD + gap]

    gapped = truth.drop(index=range(PAD, PAD + gap)).reset_index(drop=True)

    repaired = _repair_reconstruct(gapped, client)
    naive, smart = _interp_reconstruct(gapped, truth_idx)

    truth_gap = truth.set_index("open_time").loc[gap_labels, OHLCV]
    m_repair = _metrics(repaired.loc[gap_labels, OHLCV], truth_gap)
    m_naive = _metrics(naive.loc[gap_labels], truth_gap)
    m_smart = _metrics(smart.loc[gap_labels], truth_gap)

    print(f"\n{'='*78}\nGAP = {gap} минут   (BTCUSDT 1m, эталон с Binance от {BASE_START})\n{'='*78}")
    print(f"{'field':<8} {'truth_mean':>13} | "
          f"{'repair MAE':>11} | {'naiveInterp MAE':>16} {'rel%':>7} | "
          f"{'smartInterp MAE':>16} {'rel%':>7}")
    print("-" * 78)
    for col in OHLCV:
        print(f"{col:<8} {truth_gap[col].mean():>13.4f} | "
              f"{m_repair.loc[col,'MAE']:>11.2e} | "
              f"{m_naive.loc[col,'MAE']:>16.4f} {m_naive.loc[col,'rel%']:>6.2f}% | "
              f"{m_smart.loc[col,'MAE']:>16.4f} {m_smart.loc[col,'rel%']:>6.2f}%")
    print(f"{'-'*78}")
    print(f"MaxAE close: repair={m_repair.loc['close','MaxAE']:.2e}  "
          f"naive={m_naive.loc['close','MaxAE']:.2f}  smart={m_smart.loc['close','MaxAE']:.2f}")
    print(f"MaxAE volume: repair={m_repair.loc['volume','MaxAE']:.2e}  "
          f"naive={m_naive.loc['volume','MaxAE']:.2f}  smart={m_smart.loc['volume','MaxAE']:.2f}")

    # Инварианты (не сравнение методов — оно намеренно без assert):
    assert len(repaired) == len(truth), "repair изменил число строк"
    assert not repaired[OHLCV].isnull().values.any(), "repair оставил NaN"
    # repair восстанавливает точно, т.к. источник тот же (Binance):
    assert m_repair.loc["close", "MAE"] < 1e-6, "repair не восстановил close точно"
    assert m_repair.loc["volume", "MAE"] < 1e-6, "repair не восстановил volume точно"
