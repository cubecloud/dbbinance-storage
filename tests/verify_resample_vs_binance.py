"""Verify fixed resample (pg + pandas) == Binance NATIVE klines across many bars / timeframes.

Ground truth = Binance public klines API (openTime/closeTime). Checks both resample paths:
  * pg_resample_to_timeframe (SQL-side)
  * resample_to_timeframe     (pandas-side)
for 30m / 1h / 4h over a multi-day window, bar-by-bar OHLCV.

Run (env with dbbinance 0.96 + requests, .env sourced):
  set -a; for f in *.env; do source "$f"; done; set +a
  conda run -n sunday-base-213-tests python <thispath>
"""
import datetime as dt
import logging

import pandas as pd
import requests

from dbbinance.config.configpostgresql import ConfigPostgreSQL
from dbbinance.fetcher.datafetcher import DataFetcher

logging.basicConfig(level=logging.INFO, format='%(message)s')
log = logging.getLogger('verify')

W0 = dt.datetime(2024, 5, 23, 0, 0, tzinfo=dt.timezone.utc)
W1 = dt.datetime(2024, 5, 26, 0, 0, tzinfo=dt.timezone.utc)
TABLE = 'spot_data_btcusdt_1m'
COLS = ('open', 'high', 'low', 'close', 'volume')


def binance_native(interval: str) -> pd.DataFrame:
    rows, cur = [], W0
    while cur < W1:
        r = requests.get('https://api.binance.com/api/v3/klines',
                         params={'symbol': 'BTCUSDT', 'interval': interval,
                                 'startTime': int(cur.timestamp() * 1000),
                                 'endTime': int(W1.timestamp() * 1000), 'limit': 1000},
                         timeout=20)
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        rows += batch
        cur = dt.datetime.fromtimestamp(batch[-1][0] / 1000, tz=dt.timezone.utc) + dt.timedelta(seconds=1)
    df = pd.DataFrame(rows, columns=['ot', 'open', 'high', 'low', 'close', 'volume',
                                     'ct', 'qav', 'n', 'tbb', 'tbq', 'ig'])
    df['date'] = pd.to_datetime(df['ot'], unit='ms', utc=True)
    for c in COLS:
        df[c] = df[c].astype(float)
    return df.set_index('date')[list(COLS)].sort_index()


def cmp(name: str, a: pd.DataFrame, b: pd.DataFrame) -> bool:
    m = a.join(b, how='inner', lsuffix='_x', rsuffix='_y')
    # Drop the final bin: at the window edge it is a PARTIAL bar (fewer 1m rows than a full
    # bin), so its low/close/volume legitimately differ from Binance's complete kline. Not a
    # convention issue — compare only complete bins.
    if len(m):
        m = m.iloc[:-1]
    ok = True
    worst = []
    for c in COLS:
        d = (m[f'{c}_x'] - m[f'{c}_y']).abs()
        tol = max(1e-6, m[f'{c}_y'].abs().max() * 1e-7) if c == 'volume' else 0.01
        nbad = int((d > tol).sum())
        worst.append(f'{c}:maxΔ={float(d.max()):.6g}/bad={nbad}')
        if nbad:
            ok = False
            for ts in m.index[d > tol][:3]:
                log.info('    mismatch @ %s  %s_x=%.4f %s_y=%.4f', ts, c,
                         m.loc[ts, f'{c}_x'], c, m.loc[ts, f'{c}_y'])
    log.info('  %-28s bars=%d  %s  -> %s', name, len(m), '  '.join(worst),
             'MATCH' if ok else 'MISMATCH')
    return ok


if __name__ == '__main__':
    fetcher = DataFetcher(host=ConfigPostgreSQL.HOST, database=ConfigPostgreSQL.DATABASE,
                          user=ConfigPostgreSQL.USER, password=ConfigPostgreSQL.PASSWORD,
                          binance_api_key='dummy', binance_api_secret='dummy')
    all_ok = True
    for tf in ('30m', '1h', '4h'):
        nat = binance_native(tf)
        pg = fetcher.pg_resample_to_timeframe(
            table_name=TABLE, start=W0, end=W1, to_timeframe=tf, origin='start',
            use_cols=('open_time', *COLS), open_time_index=False)
        pg['date'] = pd.to_datetime(pg['open_time'], utc=True)
        pg = pg.set_index('date')[list(COLS)].sort_index()
        log.info('--- %s (native bars=%d) ---', tf, len(nat))
        all_ok &= cmp(f'pg_resample vs Binance {tf}', pg, nat)
        # pandas path. Pass explicit use_dtypes matching use_cols (the default binance_dtypes
        # includes close_time -> astype KeyError; full use_cols -> agg gets non-numeric cols).
        pdf = fetcher.resample_to_timeframe(
            table_name=TABLE, start=W0, end=W1, to_timeframe=tf, origin='start',
            use_cols=('open_time', *COLS),
            use_dtypes={c: 'float64' for c in COLS},
            open_time_index=False)
        pdf['date'] = pd.to_datetime(pdf['open_time'], utc=True)
        pdf = pdf.set_index('date')[list(COLS)].sort_index()
        all_ok &= cmp(f'pandas resample vs Binance {tf}', pdf, nat)
    log.info('OVERALL: %s', 'ALL MATCH' if all_ok else 'MISMATCH FOUND')
    raise SystemExit(0 if all_ok else 1)
