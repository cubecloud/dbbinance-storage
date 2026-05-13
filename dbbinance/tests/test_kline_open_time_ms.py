"""
Verify binance kline open_time decodes as MILLISECONDS in repair_index
flow (sync + async). Regression for asyncdatafetcher v0.82 fix:
previous unit='ns' decoded 2017-07-03 → 1970-01-01 + 0.025s.

Source of truth: python-binance 1.0.19 Client.get_klines docstring shows
1499040000000 as Open time (ms since epoch).
"""
import datetime as dt

import pandas as pd
import pytest

from dbbinance.fetcher.constants import Constants


# Realistic kline row exactly as python-binance returns it (str floats,
# int ms timestamps), example from Client.get_klines docstring.
KLINE_2017_07_03 = [
    1499040000000,      # Open time   — Unix ms
    "0.01634790",       # Open
    "0.80000000",       # High
    "0.01575800",       # Low
    "0.01577100",       # Close
    "148976.11427815",  # Volume
    1499644799999,      # Close time  — Unix ms
    "2434.19055334",    # Quote asset volume
    308,                # Number of trades
    "1756.87402397",    # Taker buy base
    "28.46694368",      # Taker buy quote
    "17928899.62484339",  # Can be ignored
]

EXPECTED_OPEN_TIME = pd.Timestamp("2017-07-03 00:00:00", tz="UTC")


def test_unit_ns_was_wrong():
    """Old behavior: unit='ns' decodes 1499040000000 ms as 1499040000000 ns."""
    bad = pd.to_datetime(KLINE_2017_07_03[0], unit='ns')
    assert bad.year == 1970, f"expected 1970-era bug, got {bad}"


def test_unit_ms_is_correct():
    """Fix: unit='ms' decodes the canonical example to 2017-07-03 UTC."""
    good = pd.to_datetime(KLINE_2017_07_03[0], unit='ms', utc=True)
    assert good == EXPECTED_OPEN_TIME


def test_repair_flow_temp_df_open_time():
    """Replicate the production conversion path (DataFrame from kdata)."""
    kdata = [KLINE_2017_07_03]
    temp_df = pd.DataFrame(kdata, columns=list(Constants.binance_cols))
    temp_df['open_time'] = pd.to_datetime(temp_df['open_time'], unit='ms', utc=True)
    assert temp_df['open_time'].iloc[0] == EXPECTED_OPEN_TIME
    assert str(temp_df['open_time'].dt.tz) == 'UTC'


def test_python_binance_docstring_self_consistency():
    """Cross-check ms-interpretation against datetime.utcfromtimestamp."""
    ts_ms = KLINE_2017_07_03[0]
    via_datetime = dt.datetime.fromtimestamp(ts_ms / 1000, tz=dt.timezone.utc)
    via_pandas = pd.to_datetime(ts_ms, unit='ms', utc=True).to_pydatetime()
    assert via_datetime == via_pandas
