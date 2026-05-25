# dbbinance-storage
Binance data tools. Fetch, store to local database and get resampled or raw data from it.
Postgresql must be installed, and database and users created BEFORE

1. check and correct the config files in 'config' directory
2. check and correct dbdata_updater.py or start.py (for docker image) for 'symbol_pairs'
3. run the dbdata_updater.py - to store data for chosen symbol_pairs from BINANCE to local postgresql database
4. then prompted for SALT for api, enter your Salt_1 key (22 chars), and your Salt_2 key (10 chars) (write it down for further usage)
5. enter the BINANCE API KEY and BINANCE API SECRET (check the instruction below)
6. then prompted for SALT for postgresql database, enter Salt key (write it down for further usage)
7. save files with .env extension to the main directory of your project or this project
8. ATTENTION! exclude *.env in your .gitignore
9. you can set your BINANCE SALT as BINANCE_KEY ENV variable (Salt_1+Salt_2), and postgresql SALT as PSGSQL_KEY ENV variable for auto confirmation

## How to get Binance API Token:
1. Register your account at Binance https://www.binance.com/?ref=CPA_004RZBKQWK
2. Go to "API Management" https://www.binance.com/en/my/settings/api-management?ref=CPA_004RZBKQWK
3. Then push the button "Create API" and select "System generated"
4. In "API restrictions" enable "Enable Spot & Margin Trading"
5. Copy & Paste here "API Key" and "Secret Key"



## Attention

Environment file for conda 'db-updater.yml' contains HARD prefix to env location. Correct it before using to create ENV

# Usage:

import dbbinance

## Resampling OHLCV data

Two ways to resample stored 1m candles to a higher timeframe:

- `resample_to_timeframe(...)` — resampling in pandas (downloads raw rows, then aggregates in Python).
- `pg_resample_to_timeframe(...)` — resampling entirely in PostgreSQL (SQL-side aggregation). Output is identical to the pandas method, but roughly **~4.3x faster** because only the aggregated rows are transferred.

Both methods share the same signature and exist on the synchronous `DataFetcher` and the asynchronous `AsyncDataFetcher`.

Synchronous (`DataFetcher`):

```python
import datetime
from datetime import timezone
from dbbinance.fetcher import DataFetcher, Constants

fetcher = DataFetcher(
    host=..., database=..., user=..., password=...,
    binance_api_key=..., binance_api_secret=...,
)

df = fetcher.pg_resample_to_timeframe(
    table_name="spot_data_btcusdt_1m",
    start=datetime.datetime(2018, 8, 1, tzinfo=timezone.utc),
    end=datetime.datetime.now(timezone.utc),
    to_timeframe="1h",
    origin="start",
    use_cols=Constants.ohlcv_cols,
    use_dtypes=Constants.ohlcv_dtypes,
    open_time_index=True,
    cached=False,
)
```

Asynchronous (`AsyncDataFetcher`):

```python
from dbbinance.fetcher import create_pool, Constants
from dbbinance.fetcher.asyncdatafetcher import AsyncDataFetcher

pool = await create_pool(host=..., database=..., user=..., password=...)
fetcher = AsyncDataFetcher(pool=pool, binance_api_key=..., binance_api_secret=...)

df = await fetcher.pg_resample_to_timeframe(
    table_name="spot_data_btcusdt_1m",
    start=...,
    end=...,
    to_timeframe="1h",
    origin="start",
    use_cols=Constants.ohlcv_cols,
    use_dtypes=Constants.ohlcv_dtypes,
    open_time_index=True,
    cached=False,
)
```

