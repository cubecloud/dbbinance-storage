# pg_resample_to_timeframe Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `pg_resample_to_timeframe` to `AsyncDataFetcher` — resamples OHLCV data entirely in PostgreSQL, producing output identical to the existing `resample_to_timeframe`.

**Architecture:** Two private helpers (`_timeframe_to_ms`, `_build_pg_resample_query`) build a parameterized two-CTE SQL query that bins rows using integer-ceiling arithmetic anchored at `start_timestamp`, then aggregates with ROW_NUMBER-based first/last semantics. The public method mirrors the signature and caching behaviour of `resample_to_timeframe`.

**Tech Stack:** Python 3.9, asyncpg, pandas, pytest-asyncio 1.1.0, PostgreSQL (any version)

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `dbbinance/fetcher/asyncdatafetcher.py` | Modify | Add import, 2 private helpers, 1 public method to `AsyncDataFetcher` |
| `dbbinance/tests/test_pg_resample_to_timeframe.py` | Create | 3 correctness tests + 1 speed test |

---

## Task 1: Add `get_timeframe_bins` import

**Files:**
- Modify: `dbbinance/fetcher/asyncdatafetcher.py:23`

- [ ] **Step 1: Extend the datautils import on line 23**

Current line 23:
```python
from dbbinance.fetcher.datautils import convert_timeframe_to_freq
```

Replace with:
```python
from dbbinance.fetcher.datautils import convert_timeframe_to_freq, get_timeframe_bins
```

- [ ] **Step 2: Verify import works**

```bash
/home/cubecloud/anaconda3/envs/dbupdater-tests/bin/python -c \
  "from dbbinance.fetcher.datautils import get_timeframe_bins; print(get_timeframe_bins('1h'))"
```

Expected output: `60`

- [ ] **Step 3: Commit**

```bash
git add dbbinance/fetcher/asyncdatafetcher.py
git commit -m "chore(async): import get_timeframe_bins for pg_resample helper"
```

---

## Task 2: Add `_timeframe_to_ms` and `_build_pg_resample_query` helpers

**Files:**
- Modify: `dbbinance/fetcher/asyncdatafetcher.py` — insert after `_get_agg_dict` (line ~1015)

- [ ] **Step 1: Write the failing test for `_timeframe_to_ms`**

Create `dbbinance/tests/test_pg_resample_to_timeframe.py`:

```python
"""
Tests for AsyncDataFetcher.pg_resample_to_timeframe:
- output identical to resample_to_timeframe (correctness)
- cached=False does not write to cache
- second cached=True call returns identical df
- pg method is faster than pandas method (speed comparison, no assertion)
"""
import asyncio
import datetime
import time
from datetime import timezone

import pytest
import pandas as pd

from dbbinance.fetcher import Constants, create_pool, floor_time
from dbbinance.fetcher.asyncdatafetcher import AsyncDataFetcher
from dbbinance.fetcher.fetchercachemanager import FetcherCacheManager
from dbbinance.config.configpostgresql import ConfigPostgreSQL

TABLE_NAME = "spot_data_btcusdt_1m"
START_DATE_STR = '01 Aug 2018'


@pytest.fixture(scope="module")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="module")
async def pool():
    _pool = await create_pool(
        host=ConfigPostgreSQL.HOST,
        database=ConfigPostgreSQL.DATABASE,
        user=ConfigPostgreSQL.USER,
        password=ConfigPostgreSQL.PASSWORD,
    )
    yield _pool
    await _pool.close()


@pytest.fixture(scope="module")
def start_datetime():
    return datetime.datetime.strptime(START_DATE_STR, '%d %b %Y').replace(tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def end_datetime():
    return floor_time(datetime.datetime.now(timezone.utc) - datetime.timedelta(minutes=1))


def test_timeframe_to_ms():
    from dbbinance.fetcher.asyncdatafetcher import AsyncDataFetcher
    assert AsyncDataFetcher._timeframe_to_ms('1m')  == 60_000
    assert AsyncDataFetcher._timeframe_to_ms('1h')  == 3_600_000
    assert AsyncDataFetcher._timeframe_to_ms('4h')  == 14_400_000
    assert AsyncDataFetcher._timeframe_to_ms('1d')  == 86_400_000
```

- [ ] **Step 2: Run test to verify it fails**

```bash
/home/cubecloud/anaconda3/envs/dbupdater-tests/bin/python -m pytest \
  dbbinance/tests/test_pg_resample_to_timeframe.py::test_timeframe_to_ms -v
```

Expected: FAIL — `AttributeError: type object 'AsyncDataFetcher' has no attribute '_timeframe_to_ms'`

- [ ] **Step 3: Add `_timeframe_to_ms` as a static method on `AsyncDataFetcher`**

Insert after `_get_agg_dict` (after line ~1015 in asyncdatafetcher.py):

```python
    @staticmethod
    def _timeframe_to_ms(to_timeframe: str) -> int:
        return get_timeframe_bins(to_timeframe) * 60_000

    def _build_pg_resample_query(
        self,
        col_type: str,
        table_name: str,
        freq_ms: int,
        use_cols: tuple,
        agg_dict: dict,
    ) -> str:
        _AGG_SQL = {
            'first': 'MAX(CASE WHEN rn_asc=1 THEN {col} END)',
            'last':  'MAX(CASE WHEN rn_desc=1 THEN {col} END)',
            'max':   'MAX({col})',
            'min':   'MIN({col})',
            'sum':   'SUM({col})',
        }

        data_cols = ', '.join(c for c in use_cols if c != 'open_time')

        if col_type == BIGINT:
            bin_formula = (
                f"$1 + ((open_time - $1 + {freq_ms} - 1) / {freq_ms}) * {freq_ms}"
            )
        else:
            bin_formula = (
                f"$1 + (CEIL(EXTRACT(EPOCH FROM (open_time - $1)) * 1000.0 "
                f"/ {freq_ms}))::bigint * interval '{freq_ms} milliseconds'"
            )

        agg_exprs = ',\n    '.join(
            _AGG_SQL[fn].format(col=col)
            for col, fn in agg_dict.items()
        )

        return f"""
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
"""
```

- [ ] **Step 4: Run test to verify it passes**

```bash
/home/cubecloud/anaconda3/envs/dbupdater-tests/bin/python -m pytest \
  dbbinance/tests/test_pg_resample_to_timeframe.py::test_timeframe_to_ms -v
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add dbbinance/fetcher/asyncdatafetcher.py \
        dbbinance/tests/test_pg_resample_to_timeframe.py
git commit -m "feat(async): add _timeframe_to_ms and _build_pg_resample_query helpers"
```

---

## Task 3: Implement `pg_resample_to_timeframe`

**Files:**
- Modify: `dbbinance/fetcher/asyncdatafetcher.py` — insert after `resample_to_timeframe` (after line ~1128)

- [ ] **Step 1: Write the failing correctness test** (append to test file)

```python
async def test_pg_resample_identical_to_pandas(pool, start_datetime, end_datetime):
    """pg_resample_to_timeframe output must be identical to resample_to_timeframe."""
    fetcher = AsyncDataFetcher(pool=pool, binance_api_key='dummy', binance_api_secret='dummy')

    pandas_df = await fetcher.resample_to_timeframe(
        table_name=TABLE_NAME, start=start_datetime, end=end_datetime,
        to_timeframe="1h", origin="start",
        use_cols=Constants.ohlcv_cols, use_dtypes=Constants.ohlcv_dtypes,
        open_time_index=True, cached=False,
    )
    pg_df = await fetcher.pg_resample_to_timeframe(
        table_name=TABLE_NAME, start=start_datetime, end=end_datetime,
        to_timeframe="1h", origin="start",
        use_cols=Constants.ohlcv_cols, use_dtypes=Constants.ohlcv_dtypes,
        open_time_index=True, cached=False,
    )

    assert not pandas_df.empty, "pandas result is empty"
    assert not pg_df.empty, "pg result is empty"
    assert list(pandas_df.columns) == list(pg_df.columns), \
        f"column mismatch: pandas={list(pandas_df.columns)} pg={list(pg_df.columns)}"
    assert len(pandas_df) == len(pg_df), \
        f"row count mismatch: pandas={len(pandas_df)} pg={len(pg_df)}"

    diff = pandas_df.compare(pg_df, align_axis=0)
    assert diff.empty, f"DataFrames differ:\n{diff.head(20)}"
```

- [ ] **Step 2: Run test to verify it fails**

```bash
/home/cubecloud/anaconda3/envs/dbupdater-tests/bin/python -m pytest \
  dbbinance/tests/test_pg_resample_to_timeframe.py::test_pg_resample_identical_to_pandas -v
```

Expected: FAIL — `AttributeError: 'AsyncDataFetcher' object has no attribute 'pg_resample_to_timeframe'`

- [ ] **Step 3: Implement `pg_resample_to_timeframe`**

Insert the following after the closing `return data_df.copy(deep=True)` of `resample_to_timeframe` (after line ~1128, before `async def main_tests()`):

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
    ) -> pd.DataFrame:
        start_timestamp, end_timestamp = await self.prepare_start_end(table_name, start, end)
        col_type = await _AsyncSchemaCache.get(self.pool, table_name)
        freq_ms = self._timeframe_to_ms(to_timeframe)
        agg_dict = self._get_agg_dict(use_cols)

        async def fetch_and_build() -> pd.DataFrame:
            sql = self._build_pg_resample_query(
                col_type=col_type,
                table_name=table_name,
                freq_ms=freq_ms,
                use_cols=use_cols,
                agg_dict=agg_dict,
            )
            async with AsyncPool(self.pool) as conn:
                records = await conn.fetch(sql, start_timestamp, end_timestamp)

            out_cols = ['open_time'] + [c for c in use_cols if c != 'open_time']
            df = pd.DataFrame(records, columns=out_cols)

            if use_dtypes:
                df = df.astype({c: use_dtypes[c] for c in use_dtypes if c in df.columns and c != 'open_time'})

            if col_type == BIGINT:
                df['open_time'] = pd.to_datetime(df['open_time'], unit='ms', utc=True)
            else:
                df['open_time'] = pd.to_datetime(df['open_time'], utc=True)

            if open_time_index:
                df = df.set_index('open_time')

            return df

        if cached:
            cache_key = self.CM.get_cache_key(
                table_name=table_name,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
                timeframe=to_timeframe,
                origin=origin,
                open_time_index=open_time_index,
                method='pg',
            )
            if cache_key in self.CM.cache:
                logger.debug(
                    f"{self.__class__.__name__} #{self.idnum}: "
                    f"Return cached PG RESAMPLED data: {table_name} / "
                    f"{start_timestamp} - {end_timestamp}"
                )
                return self.CM.cache[cache_key].copy(deep=True)

            df = await fetch_and_build()
            self.CM.update_cache(key=cache_key, value=df.copy(deep=True))
            return df.copy(deep=True)

        return await fetch_and_build()
```

- [ ] **Step 4: Run the correctness test**

```bash
/home/cubecloud/anaconda3/envs/dbupdater-tests/bin/python -m pytest \
  dbbinance/tests/test_pg_resample_to_timeframe.py::test_pg_resample_identical_to_pandas -v
```

Expected: PASS. If the diff is non-empty, inspect `diff.head(20)` — likely a dtype mismatch in `open_time` index or a float precision difference in `volume`.

**Debugging dtype mismatch:** If index dtypes differ, add before `df.compare`:
```python
pandas_df.index = pandas_df.index.astype('datetime64[ns, UTC]')
pg_df.index = pg_df.index.astype('datetime64[ns, UTC]')
```
and adjust post-processing in `pg_resample_to_timeframe` accordingly.

- [ ] **Step 5: Commit**

```bash
git add dbbinance/fetcher/asyncdatafetcher.py \
        dbbinance/tests/test_pg_resample_to_timeframe.py
git commit -m "feat(async): add pg_resample_to_timeframe — SQL-side OHLCV resampling"
```

---

## Task 4: Remaining correctness tests

**Files:**
- Modify: `dbbinance/tests/test_pg_resample_to_timeframe.py` — append two tests

- [ ] **Step 1: Append the two tests to the test file**

```python
async def test_pg_resample_no_cache_write_when_uncached(pool, start_datetime, end_datetime):
    """cached=False must not write any entry to the cache."""
    cache_manager_obj = FetcherCacheManager(max_memory_gb=1)
    fetcher = AsyncDataFetcher(
        pool=pool, binance_api_key='dummy', binance_api_secret='dummy',
        cache_obj=cache_manager_obj,
    )

    await fetcher.pg_resample_to_timeframe(
        table_name=TABLE_NAME, start=start_datetime, end=end_datetime,
        to_timeframe="1h", origin="start",
        use_cols=Constants.ohlcv_cols, use_dtypes=Constants.ohlcv_dtypes,
        open_time_index=True, cached=False,
    )

    assert len(cache_manager_obj.cache) == 0, (
        f"cached=False wrote {len(cache_manager_obj.cache)} entry(ies) to cache"
    )


async def test_pg_resample_cached_same_result(pool, start_datetime, end_datetime):
    """cached=True: second call returns identical df (cache hit)."""
    fetcher = AsyncDataFetcher(pool=pool, binance_api_key='dummy', binance_api_secret='dummy')

    first_df = await fetcher.pg_resample_to_timeframe(
        table_name=TABLE_NAME, start=start_datetime, end=end_datetime,
        to_timeframe="1h", origin="start",
        use_cols=Constants.ohlcv_cols, use_dtypes=Constants.ohlcv_dtypes,
        open_time_index=True, cached=True,
    )
    second_df = await fetcher.pg_resample_to_timeframe(
        table_name=TABLE_NAME, start=start_datetime, end=end_datetime,
        to_timeframe="1h", origin="start",
        use_cols=Constants.ohlcv_cols, use_dtypes=Constants.ohlcv_dtypes,
        open_time_index=True, cached=True,
    )

    assert not first_df.empty
    diff = first_df.compare(second_df, align_axis=0)
    assert diff.empty, f"cached=True second call differs:\n{diff}"
```

- [ ] **Step 2: Run the two new tests**

```bash
/home/cubecloud/anaconda3/envs/dbupdater-tests/bin/python -m pytest \
  dbbinance/tests/test_pg_resample_to_timeframe.py::test_pg_resample_no_cache_write_when_uncached \
  dbbinance/tests/test_pg_resample_to_timeframe.py::test_pg_resample_cached_same_result -v
```

Expected: both PASS

- [ ] **Step 3: Run full test suite for regression**

```bash
/home/cubecloud/anaconda3/envs/dbupdater-tests/bin/python -m pytest \
  dbbinance/tests/test_asyncdatafetcher.py \
  dbbinance/tests/test_pg_resample_to_timeframe.py -v
```

Expected: all 7 tests PASS (4 existing + 3 new)

- [ ] **Step 4: Commit**

```bash
git add dbbinance/tests/test_pg_resample_to_timeframe.py
git commit -m "test(async): correctness tests for pg_resample_to_timeframe"
```

---

## Task 5: Speed comparison test

**Files:**
- Modify: `dbbinance/tests/test_pg_resample_to_timeframe.py` — append speed test

- [ ] **Step 1: Append the speed test**

```python
async def test_speed_comparison(pool, start_datetime, end_datetime):
    """Compare wall-clock time: pg vs pandas resampling. Prints results, no assertion."""
    fetcher = AsyncDataFetcher(pool=pool, binance_api_key='dummy', binance_api_secret='dummy')
    N = 3

    # warm-up (fills any OS/PG caches, not timed)
    await fetcher.resample_to_timeframe(
        table_name=TABLE_NAME, start=start_datetime, end=end_datetime,
        to_timeframe="1h", origin="start",
        use_cols=Constants.ohlcv_cols, use_dtypes=Constants.ohlcv_dtypes,
        open_time_index=True, cached=False,
    )
    await fetcher.pg_resample_to_timeframe(
        table_name=TABLE_NAME, start=start_datetime, end=end_datetime,
        to_timeframe="1h", origin="start",
        use_cols=Constants.ohlcv_cols, use_dtypes=Constants.ohlcv_dtypes,
        open_time_index=True, cached=False,
    )

    pandas_times = []
    for _ in range(N):
        t0 = time.perf_counter()
        await fetcher.resample_to_timeframe(
            table_name=TABLE_NAME, start=start_datetime, end=end_datetime,
            to_timeframe="1h", origin="start",
            use_cols=Constants.ohlcv_cols, use_dtypes=Constants.ohlcv_dtypes,
            open_time_index=True, cached=False,
        )
        pandas_times.append((time.perf_counter() - t0) * 1000)

    pg_times = []
    for _ in range(N):
        t0 = time.perf_counter()
        await fetcher.pg_resample_to_timeframe(
            table_name=TABLE_NAME, start=start_datetime, end=end_datetime,
            to_timeframe="1h", origin="start",
            use_cols=Constants.ohlcv_cols, use_dtypes=Constants.ohlcv_dtypes,
            open_time_index=True, cached=False,
        )
        pg_times.append((time.perf_counter() - t0) * 1000)

    pandas_avg = sum(pandas_times) / N
    pg_avg = sum(pg_times) / N
    speedup = pandas_avg / pg_avg if pg_avg > 0 else float('inf')

    print(f"\n{'Method':<25} {'Run 1':>8} {'Run 2':>8} {'Run 3':>8} {'Avg':>8}")
    print("-" * 60)
    print(f"{'resample_to_timeframe':<25} " +
          " ".join(f"{t:>7.0f}ms" for t in pandas_times) +
          f" {pandas_avg:>7.0f}ms")
    print(f"{'pg_resample_to_timeframe':<25} " +
          " ".join(f"{t:>7.0f}ms" for t in pg_times) +
          f" {pg_avg:>7.0f}ms")
    print(f"\nSpeedup: {speedup:.2f}x  ({'faster' if speedup > 1 else 'slower'})")
```

- [ ] **Step 2: Run the speed test**

```bash
/home/cubecloud/anaconda3/envs/dbupdater-tests/bin/python -m pytest \
  dbbinance/tests/test_pg_resample_to_timeframe.py::test_speed_comparison -v -s
```

Expected: PASS, prints timing table. The `-s` flag shows stdout so the table is visible.

- [ ] **Step 3: Run the complete test suite**

```bash
/home/cubecloud/anaconda3/envs/dbupdater-tests/bin/python -m pytest \
  dbbinance/tests/test_asyncdatafetcher.py \
  dbbinance/tests/test_pg_resample_to_timeframe.py -v -s
```

Expected: all 8 tests PASS

- [ ] **Step 4: Commit**

```bash
git add dbbinance/tests/test_pg_resample_to_timeframe.py
git commit -m "test(async): add speed comparison test for pg_resample_to_timeframe"
```

---

## Self-Review

**Spec coverage:**
- ✅ Bin formula (BIGINT + TIMESTAMPTZ) — Task 2
- ✅ Two-CTE SQL structure + agg mapping — Task 2
- ✅ Same method signature — Task 3
- ✅ Post-processing (dtype, datetime conversion, index) — Task 3
- ✅ Caching with `method='pg'` key — Task 3
- ✅ `test_pg_resample_identical_to_pandas` — Task 3
- ✅ `test_pg_resample_no_cache_write_when_uncached` — Task 4
- ✅ `test_pg_resample_cached_same_result` — Task 4
- ✅ Speed comparison test — Task 5

**Type consistency:**
- `_timeframe_to_ms` is `@staticmethod` → called as `self._timeframe_to_ms(...)` ✓
- `_build_pg_resample_query` is instance method, consistent with `self` usage ✓
- Cache key uses `CM.get_cache_key(**kwargs)` → returns sorted tuple, consistent with existing usage ✓
- `AsyncPool(self.pool)` pattern matches `fetch_raw_data` in `resample_to_timeframe` ✓

**Placeholder scan:** None found.
