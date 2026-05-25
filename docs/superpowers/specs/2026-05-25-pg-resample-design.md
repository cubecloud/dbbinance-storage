# pg_resample_to_timeframe — Design Spec

**Date:** 2026-05-25  
**Branch:** fix/async-resample-cache  
**Status:** Approved

---

## Goal

Add `pg_resample_to_timeframe` to `AsyncDataFetcher` — a method that resamples OHLCV
data entirely in PostgreSQL using a single SQL query, instead of fetching all raw rows
and resampling in pandas. Must produce byte-identical output to `resample_to_timeframe`.

---

## Bin Semantics

`origin='start'` — bins are anchored at `start_timestamp`, not the Unix epoch.

Example: `start=12:02, timeframe=1h` → bin labels: `12:02, 13:02, 14:02, ...`

- Bin labeled `T` contains all rows where `(T - freq, T]` (right-closed, left-open).
- The row at exactly `start` falls in the first bin labeled `start`.

This matches pandas `resample(freq, label='right', closed='right', origin='start')`.

---

## Bin Formula

### BIGINT schema (open_time in Unix milliseconds)

```
bin_label = start_ms + CEIL((open_time - start_ms) / freq_ms) * freq_ms
```

Integer CEIL without floating point:
```sql
$start + ((open_time - $start + {freq_ms} - 1) / {freq_ms}) * {freq_ms} AS bin_label
```

Where `{freq_ms}` is embedded as a Python f-string literal (computed from `to_timeframe`).

Verification:
- `delta=0` (open_time=start): `(0 + freq-1)/freq = 0` → label = start ✓
- `delta=1`: `(1+freq-1)/freq = 1` → label = start+freq ✓  
- `delta=freq` (exactly on boundary): `(freq+freq-1)/freq = 1` → label = start+freq ✓
- `delta=freq+1`: `(freq+1+freq-1)/freq = 2` → label = start+2*freq ✓

### TIMESTAMPTZ schema

```sql
$start + CEIL(
    EXTRACT(EPOCH FROM (open_time - $start)) * 1000.0 / {freq_ms}
)::bigint * interval '{freq_ms} milliseconds' AS bin_label
```

Uses float CEIL on epoch-ms offset, then converts back to interval. Avoids float-precision
issues because Binance 1m candle boundaries are always on exact second boundaries.

---

## SQL Query Structure

Two-CTE pattern avoids repeating the bin formula and keeps the query readable:

```sql
WITH pre AS (
    SELECT
        {bin_formula} AS bin_label,
        open_time,
        {data_cols}
    FROM {table_name}
    WHERE open_time >= $1 AND open_time <= $2
),
binned AS (
    SELECT
        bin_label, open_time, {data_cols},
        ROW_NUMBER() OVER (PARTITION BY bin_label ORDER BY open_time ASC)  AS rn_asc,
        ROW_NUMBER() OVER (PARTITION BY bin_label ORDER BY open_time DESC) AS rn_desc
    FROM pre
)
SELECT
    bin_label AS open_time,
    {agg_exprs}
FROM binned
GROUP BY bin_label
ORDER BY bin_label
```

### agg_dict → SQL mapping

| pandas agg | SQL expression |
|---|---|
| `'first'` | `MAX(CASE WHEN rn_asc=1 THEN {col} END)` |
| `'last'`  | `MAX(CASE WHEN rn_desc=1 THEN {col} END)` |
| `'max'`   | `MAX({col})` |
| `'min'`   | `MIN({col})` |
| `'sum'`   | `SUM({col})` |

`open_time` is excluded from agg (it becomes the bin label). `_get_agg_dict` already does this.

---

## Method Signature

```python
async def pg_resample_to_timeframe(
    self,
    table_name: str,
    start: Union[datetime.datetime, int],
    end: Union[datetime.datetime, int],
    to_timeframe: str = "1h",
    origin: str = "start",
    use_cols: tuple = Constants.binance_cols,
    use_dtypes=None,
    open_time_index: bool = True,
    cached: bool = False,
) -> pd.DataFrame
```

Identical signature to `resample_to_timeframe`.

---

## Implementation Steps

1. **`_timeframe_to_ms(to_timeframe)`** — helper, maps timeframe string to milliseconds.
   Uses existing `get_timeframe_bins(to_timeframe)` (returns minutes) × 60_000.

2. **`_build_pg_resample_query`** — private method that builds the SQL string given
   `col_type`, `table_name`, `freq_ms`, `use_cols`, `agg_dict`.

3. **`pg_resample_to_timeframe`** — main method:
   - `prepare_start_end` → typed (start, end)
   - `_AsyncSchemaCache.get` → col_type
   - `_timeframe_to_ms` → freq_ms
   - `_get_agg_dict` → agg_dict (same as existing, excludes open_time)
   - `_build_pg_resample_query` → sql
   - Execute via `AsyncPool`, build DataFrame
   - Post-process: apply `use_dtypes`, convert open_time to datetime (BIGINT path),
     set/reset index per `open_time_index`
   - Cache via `CM` with key that includes `method='pg'` to avoid collision with
     pandas cache entries

---

## Post-processing (must match resample_to_timeframe output)

```python
df = pd.DataFrame(records, columns=['open_time', ...])
df = df.astype({c: use_dtypes[c] for c in use_dtypes if c != 'open_time'})
# BIGINT path: open_time is int(ms) from SQL → convert to datetime
if col_type == BIGINT:
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms', utc=True)
# TIMESTAMPTZ path: asyncpg returns datetime → to_datetime for consistency
else:
    df['open_time'] = pd.to_datetime(df['open_time'], utc=True)
if open_time_index:
    df = df.set_index('open_time')
```

---

## Tests

### File: `dbbinance/tests/test_pg_resample_to_timeframe.py`

**Correctness tests** (hit real DB, same pattern as existing tests):

1. `test_pg_resample_identical_to_pandas` — compare with `df.compare()`, assert empty diff
2. `test_pg_resample_no_cache_write_when_uncached` — `cached=False` → `len(cache) == 0`
3. `test_pg_resample_cached_same_result` — second call returns identical df

**Speed tests** (real DB, 3 timed runs each, print results, no timing assertion):

4. `test_speed_comparison` — warm run + 3 timed runs of each method, print ms timing table

---

## Files to Create/Modify

| File | Change |
|---|---|
| `dbbinance/fetcher/asyncdatafetcher.py` | Add `_timeframe_to_ms`, `_build_pg_resample_query`, `pg_resample_to_timeframe` to `AsyncDataFetcher` |
| `dbbinance/tests/test_pg_resample_to_timeframe.py` | New test file |
