#!/usr/bin/env python
"""
One-shot backfill for frozen (partial) OHLCV bars.

Background: the live updater (async_start.py / sync_start.py) persisted the CURRENT
unclosed minute with near-zero volume, while ON CONFLICT DO NOTHING + the next run
starting at max_open_time+1 never let it be corrected. Result: bars with volume=0,
close=open, frozen forever. See docs/HANDOFF_partial_bar_freeze_2026-06-25.md (item C).

What this script does: it receives the minutes to re-read and rewrite (a [--start, --end)
range -> picks volume=0 bars, or an explicit --minutes list). For each minute it re-fetches
the CLOSED candle from Binance and rewrites it into the DB via upsert. Every step is logged;
every rewritten bar is logged in full (before -> after); after the write the row is re-read
and verified.

KEY SAFETY: a rewrite happens ONLY if Binance returned a candle with volume > 0. If there is
no candle (exchange halt, e.g. 2023-03-24) or Binance also reports volume == 0 (a genuine
empty minute, e.g. illiquid 2017), the minute is SKIPPED and left untouched. The authority is
Binance's response for that specific minute, NOT the input list.

Default mode is dry-run (plan only). Real writes happen only with --apply.

Technically: pure psycopg2 (sync), %s placeholders. No asyncio / asyncpg.

Examples:
  # dry-run over a period (diagnose which bars are actually frozen)
  python backfill_frozen_bars.py --start 2023-11-01 --end 2024-01-01
  # real repair of a period
  python backfill_frozen_bars.py --start 2023-11-01 --end 2024-01-01 --apply
"""

import os
import sys
import glob
import time
import logging
import argparse
import datetime
from datetime import timezone

REPO = os.path.dirname(os.path.abspath(__file__))

# Field indices in a Binance get_historical_klines candle
K_OPEN_TIME = 0
K_OPEN = 1
K_HIGH = 2
K_LOW = 3
K_CLOSE = 4
K_VOLUME = 5
K_CLOSE_TIME = 6
K_QUOTE_VOLUME = 7
K_TRADES = 8
K_TAKER_BASE = 9
K_TAKER_QUOTE = 10
K_IGNORED = 11

logger = logging.getLogger("backfill_frozen_bars")


# ---------------------------------------------------------------------------
# environment / logging
# ---------------------------------------------------------------------------
def load_env_files() -> list:
    """Load every *.env in the repo root into os.environ."""
    loaded = []
    for path in sorted(glob.glob(os.path.join(REPO, "*.env"))):
        with open(path) as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                os.environ[key.strip()] = value.strip().strip('"').strip("'")
        loaded.append(os.path.basename(path))
    return loaded


def setup_logging(log_path: str) -> None:
    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    logger.setLevel(logging.DEBUG)
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(fmt)
    logger.addHandler(console)
    fileh = logging.FileHandler(log_path)
    fileh.setLevel(logging.DEBUG)
    fileh.setFormatter(fmt)
    logger.addHandler(fileh)
    logger.info("Logging to %s", log_path)


# ---------------------------------------------------------------------------
# Binance
# ---------------------------------------------------------------------------
def fetch_closed_kline(client, symbol: str, timeframe: str, minute_ms: int):
    """
    Return a SINGLE Binance candle for the exact minute minute_ms (open_time, ms).
    Returns the kline (list) or None if there is no candle.
    """
    klines = client.get_historical_klines(
        symbol, timeframe,
        start_str=minute_ms,
        end_str=minute_ms + 59_000,
        limit=1,
    )
    if not klines:
        return None
    return klines[0]


# ---------------------------------------------------------------------------
# helper: format a bar row for the log
# ---------------------------------------------------------------------------
def fmt_row(row) -> str:
    if row is None:
        return "<no row>"
    return ("o=%.2f h=%.2f l=%.2f c=%.2f vol=%.6f trades=%d qav=%.4f tbb=%.6f tbq=%.4f"
            % (float(row["open"]), float(row["high"]), float(row["low"]),
               float(row["close"]), float(row["volume"]), int(row["trades"]),
               float(row["quote_asset_volume"]), float(row["taker_buy_base"]),
               float(row["taker_buy_quote"])))


SELECT_COLS = ("open, high, low, close, volume, close_time, "
               "quote_asset_volume, trades, taker_buy_base, taker_buy_quote, ignored")


# ---------------------------------------------------------------------------
# single minute: classify + (optional) write + verify
# ---------------------------------------------------------------------------
def process_minute(conn, cur, client, table, symbol, timeframe, open_time, apply):
    """
    Process one minute. Returns a category string:
      FIX                - rewritten (binance volume > 0), verified
      DRY_FIX            - same as FIX but without --apply (no write performed)
      SKIP_GENUINE_ZERO  - Binance also reports volume == 0 (genuine empty minute)
      SKIP_NO_KLINE      - Binance returned no candle (halt / minute absent on exchange)
      SKIP_UNCLOSED      - candle not closed yet (close_time >= now)
      SKIP_MISMATCH_MIN  - Binance returned a different minute (off-by-one guard)
      MISMATCH           - after the write the row did not match the fetched values
    """
    open_dt = open_time.astimezone(timezone.utc)
    minute_ms = int(open_dt.timestamp() * 1000)

    cur.execute(f"SELECT {SELECT_COLS} FROM {table} WHERE open_time = %s", (open_time,))
    before = cur.fetchone()
    before_vol = float(before["volume"]) if before else None

    kline = fetch_closed_kline(client, symbol, timeframe, minute_ms)
    if kline is None:
        logger.info("SKIP_NO_KLINE   %s  db_vol=%s  (exchange returned no candle: halt/absent)",
                    open_dt.strftime("%Y-%m-%d %H:%M"), before_vol)
        return "SKIP_NO_KLINE"

    k_open_ms = int(kline[K_OPEN_TIME])
    k_close_ms = int(kline[K_CLOSE_TIME])
    k_vol = float(kline[K_VOLUME])

    # off-by-one: Binance must return exactly the requested minute
    if k_open_ms != minute_ms:
        logger.warning("SKIP_MISMATCH_MIN  %s  requested=%d got=%d -> skip",
                       open_dt.strftime("%Y-%m-%d %H:%M"), minute_ms, k_open_ms)
        return "SKIP_MISMATCH_MIN"

    # the candle must be closed
    now_ms = int(datetime.datetime.now(timezone.utc).timestamp() * 1000)
    if k_close_ms >= now_ms:
        logger.info("SKIP_UNCLOSED   %s  (close_time>=now, candle still forming)",
                    open_dt.strftime("%Y-%m-%d %H:%M"))
        return "SKIP_UNCLOSED"

    # genuine empty minute - leave untouched
    if k_vol == 0:
        logger.info("SKIP_GENUINE_ZERO  %s  db_vol=%s binance_vol=0 (genuine empty minute)",
                    open_dt.strftime("%Y-%m-%d %H:%M"), before_vol)
        return "SKIP_GENUINE_ZERO"

    # --- this is a freeze: Binance has volume, DB has zeros ---
    close_dt = datetime.datetime.fromtimestamp(k_close_ms / 1000, tz=timezone.utc)
    values = (
        open_dt,
        float(kline[K_OPEN]),
        float(kline[K_HIGH]),
        float(kline[K_LOW]),
        float(kline[K_CLOSE]),
        k_vol,
        close_dt,
        float(kline[K_QUOTE_VOLUME]),
        int(kline[K_TRADES]),
        float(kline[K_TAKER_BASE]),
        float(kline[K_TAKER_QUOTE]),
        float(kline[K_IGNORED]),
    )

    if not apply:
        logger.info("DRY_FIX         %s  db_vol=%s -> binance_vol=%.4f  close %s->%.2f (not written)",
                    open_dt.strftime("%Y-%m-%d %H:%M"), before_vol, k_vol,
                    float(before["close"]) if before else None, float(kline[K_CLOSE]))
        return "DRY_FIX"

    # upsert (DO UPDATE on all columns), commit per row (idempotent)
    cur.execute(
        f"""
        INSERT INTO {table} (
            open_time, open, high, low, close, volume, close_time,
            quote_asset_volume, trades, taker_buy_base, taker_buy_quote, ignored
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (open_time) DO UPDATE SET
            open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
            close = EXCLUDED.close, volume = EXCLUDED.volume, close_time = EXCLUDED.close_time,
            quote_asset_volume = EXCLUDED.quote_asset_volume, trades = EXCLUDED.trades,
            taker_buy_base = EXCLUDED.taker_buy_base, taker_buy_quote = EXCLUDED.taker_buy_quote,
            ignored = EXCLUDED.ignored
        """,
        values,
    )
    conn.commit()

    # verify: re-read the row and compare
    cur.execute(f"SELECT {SELECT_COLS} FROM {table} WHERE open_time = %s", (open_time,))
    after = cur.fetchone()
    ok = (
        after is not None
        and abs(float(after["volume"]) - k_vol) < 1e-9
        and abs(float(after["close"]) - float(kline[K_CLOSE])) < 1e-9
        and int(after["trades"]) == int(kline[K_TRADES])
    )
    if ok:
        # full record of EVERY rewritten bar (before -> after) for audit
        logger.info(
            "FIX VERIFIED    %s [%s %s]\n    BEFORE %s\n    AFTER  %s",
            open_dt.strftime("%Y-%m-%d %H:%M:%S"), table, symbol,
            fmt_row(before), fmt_row(after))
        return "FIX"

    logger.error("MISMATCH        %s  row did not match after write! after=%s",
                 open_dt, dict(after) if after else None)
    return "MISMATCH"


# ---------------------------------------------------------------------------
# input minute selection
# ---------------------------------------------------------------------------
def parse_minute(s: str) -> datetime.datetime:
    s = s.strip()
    dt = datetime.datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def select_minutes(cur, table, args) -> list:
    """Return a sorted list of open_time values to process."""
    if args.minutes:
        mins = [parse_minute(x) for x in args.minutes.split(",") if x.strip()]
        mins.sort()
        return mins
    if args.minutes_file:
        mins = []
        with open(args.minutes_file) as fh:
            for line in fh:
                line = line.strip()
                if line and not line.startswith("#"):
                    mins.append(parse_minute(line))
        mins.sort()
        return mins
    # by range: candidates are volume=0 bars in [start, end)
    if not (args.start and args.end):
        raise SystemExit("Provide either --minutes/--minutes-file, or --start and --end")
    start = parse_minute(args.start)
    end = parse_minute(args.end)
    where_vol = "" if args.all_in_range else "AND volume = 0"
    cur.execute(
        f"""SELECT open_time FROM {table}
            WHERE open_time >= %s AND open_time < %s {where_vol}
            ORDER BY open_time""",
        (start, end))
    return [r["open_time"] for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def run(args):
    import psycopg2
    import psycopg2.extras
    from binance.client import Client
    from dbbinance.config.configpostgresql import ConfigPostgreSQL

    client = Client()  # public klines endpoint, no API keys needed
    conn = psycopg2.connect(
        host=ConfigPostgreSQL.HOST, port=ConfigPostgreSQL.PORT,
        dbname=ConfigPostgreSQL.DATABASE, user=ConfigPostgreSQL.USER,
        password=ConfigPostgreSQL.PASSWORD,
    )
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    logger.info("DB: %s/%s  table=%s  symbol=%s timeframe=%s  apply=%s",
                ConfigPostgreSQL.HOST, ConfigPostgreSQL.DATABASE, args.table,
                args.symbol, args.timeframe, args.apply)
    try:
        minutes = select_minutes(cur, args.table, args)
        if args.limit:
            minutes = minutes[: args.limit]
        logger.info("Minutes to process: %d", len(minutes))
        if not minutes:
            logger.info("Nothing to process. Exit.")
            return

        counts = {}
        for i, open_time in enumerate(minutes, 1):
            cat = process_minute(conn, cur, client, args.table, args.symbol,
                                 args.timeframe, open_time, args.apply)
            counts[cat] = counts.get(cat, 0) + 1
            if i % 200 == 0:
                logger.info("... progress %d/%d  %s", i, len(minutes), counts)
            if args.sleep:
                time.sleep(args.sleep)

        logger.info("=" * 60)
        logger.info("SUMMARY (apply=%s):", args.apply)
        for cat in sorted(counts):
            logger.info("  %-18s %d", cat, counts[cat])
        logger.info("Total processed: %d", len(minutes))
        if not args.apply:
            logger.info("This was a DRY-RUN. Fixable: %d. Re-run with --apply to write.",
                        counts.get("DRY_FIX", 0))
    finally:
        cur.close()
        conn.close()


def build_parser():
    p = argparse.ArgumentParser(description="One-shot backfill for frozen OHLCV bars")
    p.add_argument("--table", default="spot_data_btcusdt_1m", help="DB table")
    p.add_argument("--symbol", default="BTCUSDT", help="pair to query on Binance")
    p.add_argument("--timeframe", default="1m", help="timeframe to query on Binance")
    p.add_argument("--start", help="range start UTC ISO (inclusive), e.g. 2023-11-01")
    p.add_argument("--end", help="range end UTC ISO (exclusive), e.g. 2024-01-01")
    p.add_argument("--minutes", help="explicit comma-separated UTC ISO minutes (overrides range)")
    p.add_argument("--minutes-file", help="file with UTC ISO minutes (one per line)")
    p.add_argument("--all-in-range", action="store_true",
                   help="take ALL minutes in range, not only volume=0")
    p.add_argument("--limit", type=int, help="cap the number of minutes (for testing)")
    p.add_argument("--sleep", type=float, default=0.0, help="pause between requests, seconds")
    p.add_argument("--apply", action="store_true",
                   help="actually write to the DB (dry-run by default)")
    p.add_argument("--log", default=None, help="log file path")
    return p


def main():
    args = build_parser().parse_args()
    load_env_files()
    log_path = args.log or os.path.join(
        REPO, "backfill_frozen_bars_%s.log" % ("apply" if args.apply else "dryrun"))
    setup_logging(log_path)
    run(args)


if __name__ == "__main__":
    main()
