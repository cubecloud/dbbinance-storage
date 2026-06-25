import os
import time
import pytz
import logging
import datetime
import pandas as pd
from typing import Tuple, Union, Optional, List, Any
from datetime import timezone

from pandas import Timestamp

from dbbinance.fetcher.constants import Constants
from dbbinance.fetcher.sqlbase import SQLMeta, handle_errors, sql, ThreadPool
from dbbinance.fetcher.fetchercachemanager import FetcherCacheManager
from dbbinance.fetcher.datautils import convert_timeframe_to_freq, get_timeframe_bins
from collections import OrderedDict
from dotenv import load_dotenv, find_dotenv

from binance.client import Client
from binance.exceptions import BinanceAPIException

from apscheduler.schedulers.background import BackgroundScheduler

__version__ = '1.0.3'  # freeze fix A+B; update_spot_data fetch window widened to 365*5 days

logger = logging.getLogger()

logger.setLevel(logging.INFO)

load_dotenv(find_dotenv())


# ----------------------------------------------------------------------
# Schema detection helpers — open_time may be BIGINT (legacy Unix-ms) or
# TIMESTAMPTZ (new schema). Methods below adapt at runtime so the same
# code works against both without consumer changes.
# ----------------------------------------------------------------------

BIGINT = "bigint"
TIMESTAMPTZ = "timestamp with time zone"


class _SchemaCache:
    """Per-process cache of open_time column data_type per (db_mgr, table).

    Schema does not change at runtime in normal operation; one
    information_schema lookup per (manager, table) is sufficient.
    """

    _types: dict = {}

    @classmethod
    def get(cls, db_mgr, table_name: str) -> str:
        key = (id(db_mgr), table_name)
        if key not in cls._types:
            query = sql.SQL(
                "SELECT data_type FROM information_schema.columns "
                "WHERE table_name = %s AND column_name = 'open_time'"
            )
            result = db_mgr.single_select_query(query, (table_name,))
            if result and result[0]:
                cls._types[key] = str(result[0]).lower()
            else:
                # Conservative default: assume the new schema.
                cls._types[key] = TIMESTAMPTZ
        return cls._types[key]

    @classmethod
    def invalidate(cls, db_mgr=None, table_name: Optional[str] = None) -> None:
        if db_mgr is None and table_name is None:
            cls._types.clear()
            return
        keep = {}
        for k, v in cls._types.items():
            if db_mgr is not None and k[0] == id(db_mgr):
                continue
            if table_name is not None and k[1] == table_name:
                continue
            keep[k] = v
        cls._types = keep


def _open_time_result_to_ms(v: Any) -> int:
    """Normalize MIN/MAX(open_time) result to int Unix milliseconds.

    BIGINT column returns int(ms); TIMESTAMPTZ column returns
    datetime.datetime. Accept both, return int.
    """
    if isinstance(v, (int, float)):
        return int(v)
    if hasattr(v, "timestamp"):
        return int(v.timestamp() * 1000)
    raise TypeError(
        f"Cannot convert open_time result of type {type(v).__name__} to ms"
    )


def ceil_time(dt=None, ceil_to='1h') -> datetime.datetime:
    if dt is None:
        dt = datetime.datetime.now()
    dt = pd.Timestamp(dt)
    freq_map = {'h': 'H', 'm': 'T', 's': 'S', 'd': 'D', 'W': 'W', 'M': 'M'}
    unit = ceil_to[-1]
    value = int(ceil_to[:-1])
    freq = f'{value}{freq_map[unit]}'
    return dt.ceil(freq).to_pydatetime()


def floor_time(dt=None, floor_to='1h', tz=None) -> datetime.datetime:
    if dt is None:
        dt = datetime.datetime.now(tz)
    dt = pd.Timestamp(dt)
    freq_map = {'h': 'H', 'm': 'T', 's': 'S', 'd': 'D', 'W': 'W', 'M': 'M'}
    unit = floor_to[-1]
    value = int(floor_to[:-1])
    freq = f'{value}{freq_map[unit]}'
    return (dt - pd.Timedelta(1, unit='ns')).floor(freq).to_pydatetime()


# Number of trailing closed bars re-read on every live update, so a partial bar that
# slipped through earlier is re-fetched closed and corrected by the upsert (B: tail overlap).
LIVE_OVERLAP_BARS = 5


def drop_unclosed_klines(klines: list) -> list:
    """Return only closed candles (close_time < now).

    Binance returns the current still-forming minute as the last element; persisting it
    freezes a partial bar (volume~0) that ON CONFLICT can never correct (A: partial-bar
    freeze fix). Expects a list of klines with close_time (ms) at index 6."""
    if not klines:
        return klines
    now_ms = int(datetime.datetime.now(timezone.utc).timestamp() * 1000)
    return [kline for kline in klines if int(kline[6]) < now_ms]


class PostgreSQLDatabase(SQLMeta):
    def __init__(self, host, database, user, password, max_conn=10):
        super().__init__(host, database, user, password, max_conn)
        self.max_verbose = False

    # ------------------------------------------------------------------
    # Table naming
    # ------------------------------------------------------------------
    @staticmethod
    def prepare_table_name(
            market: str = "spot",
            symbol_pair: str = "BTCUSDT",
            timeframe: str = "1m",
    ) -> str:
        return f"{market}_data_{symbol_pair}_{timeframe}".lower()

    # ------------------------------------------------------------------
    # Table creation (optimized schema)
    # ------------------------------------------------------------------
    @handle_errors
    def create_table(self, table_name: str, use_brin: bool = True):
        """
        Creates a new optimized OHLCV table with indexes.
        Args:
            table_name (str): Target table name.
            use_brin (bool): Whether to create a fresh BRIN index on open_time
        """
        # -----------------------------
        # Create table
        # -----------------------------
        query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {table} (
                open_time TIMESTAMPTZ NOT NULL PRIMARY KEY,

                open   FLOAT8 NOT NULL,
                high   FLOAT8 NOT NULL,
                low    FLOAT8 NOT NULL,
                close  FLOAT8 NOT NULL,
                volume FLOAT8 NOT NULL,

                close_time TIMESTAMPTZ NOT NULL,

                quote_asset_volume FLOAT8,
                trades              INTEGER,
                taker_buy_base      FLOAT8,
                taker_buy_quote     FLOAT8,
                ignored             FLOAT8
            );
        """).format(table=sql.Identifier(table_name))

        if not self.db_mgr.modify_query(query):
            raise RuntimeError(f"Failed to create table {table_name}")
        logger.info(f"{self.__class__.__name__}: Table '{table_name}' ready")

        # -----------------------------
        # Drop old BRIN index if exists
        # -----------------------------
        if use_brin:
            brin_idx_name = f"{table_name}_open_time_brin"

            drop_query = sql.SQL("DROP INDEX IF EXISTS {idx}").format(
                idx=sql.Identifier(brin_idx_name)
            )
            self.db_mgr.modify_query(drop_query)
            logger.debug(f"{self.__class__.__name__}: Dropped old BRIN index '{brin_idx_name}' (if existed)")

            # -----------------------------
            # 3️⃣ Create fresh BRIN index
            # -----------------------------
            brin_query = sql.SQL("""
                CREATE INDEX {idx}
                ON {table}
                USING BRIN (open_time);
            """).format(
                idx=sql.Identifier(brin_idx_name),
                table=sql.Identifier(table_name)
            )
            if self.db_mgr.modify_query(brin_query):
                logger.info(f"{self.__class__.__name__}: BRIN index on '{table_name}.open_time' created")
            else:
                logger.warning(f"{self.__class__.__name__}: Failed to create BRIN index on '{table_name}'")

    # ------------------------------------------------------------------
    # Single insert (safe, idempotent)
    # ------------------------------------------------------------------
    def insert_kline_to_table(self, table_name: str, kline: list):
        open_time_ms, open_, high, low, close, volume, close_time_ms, \
            quote_asset_volume, trades, taker_buy_base, taker_buy_quote, ignored = kline[:12]

        # A (partial-bar freeze fix): skip the still-forming candle (close_time >= now).
        now_ms = int(datetime.datetime.now(timezone.utc).timestamp() * 1000)
        if int(close_time_ms) >= now_ms:
            return

        row = (
            datetime.datetime.fromtimestamp(open_time_ms / 1000, tz=timezone.utc),
            float(open_),
            float(high),
            float(low),
            float(close),
            float(volume),
            datetime.datetime.fromtimestamp(close_time_ms / 1000, tz=timezone.utc),
            float(quote_asset_volume),
            int(trades),
            float(taker_buy_base),
            float(taker_buy_quote),
            float(ignored),
        )

        query = sql.SQL("""
            INSERT INTO {table} (
                open_time, open, high, low, close, volume,
                close_time, quote_asset_volume, trades,
                taker_buy_base, taker_buy_quote, ignored
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (open_time) DO UPDATE SET
                open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
                close = EXCLUDED.close, volume = EXCLUDED.volume, close_time = EXCLUDED.close_time,
                quote_asset_volume = EXCLUDED.quote_asset_volume, trades = EXCLUDED.trades,
                taker_buy_base = EXCLUDED.taker_buy_base, taker_buy_quote = EXCLUDED.taker_buy_quote,
                ignored = EXCLUDED.ignored;
        """).format(table=sql.Identifier(table_name))

        self.db_mgr.modify_query(query, row)

    # ------------------------------------------------------------------
    # Bulk insert (fast, no table locks)
    # ------------------------------------------------------------------
    def insert_klines_to_table(self, table_name: str, klines: list):
        # A (partial-bar freeze fix): drop the still-forming candle before persisting.
        klines = drop_unclosed_klines(klines)
        if not klines:
            return
        rows = [
            (
                datetime.datetime.fromtimestamp(k[0] / 1000, tz=timezone.utc),
                float(k[1]),
                float(k[2]),
                float(k[3]),
                float(k[4]),
                float(k[5]),
                datetime.datetime.fromtimestamp(k[6] / 1000, tz=timezone.utc),
                float(k[7]),
                int(k[8]),
                float(k[9]),
                float(k[10]),
                float(k[11]),
            )
            for k in klines
        ]

        query = sql.SQL("""
            INSERT INTO {table} (
                open_time, open, high, low, close, volume,
                close_time, quote_asset_volume, trades,
                taker_buy_base, taker_buy_quote, ignored
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (open_time) DO UPDATE SET
                open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
                close = EXCLUDED.close, volume = EXCLUDED.volume, close_time = EXCLUDED.close_time,
                quote_asset_volume = EXCLUDED.quote_asset_volume, trades = EXCLUDED.trades,
                taker_buy_base = EXCLUDED.taker_buy_base, taker_buy_quote = EXCLUDED.taker_buy_quote,
                ignored = EXCLUDED.ignored;
        """).format(table=sql.Identifier(table_name))

        with ThreadPool(self.pool) as conn:
            with conn.cursor() as cur:
                cur.executemany(query, rows)
            conn.commit()

    # ------------------------------------------------------------------
    # Debug helper
    # ------------------------------------------------------------------
    def logger_debug(self, msg):
        if self.max_verbose:
            logger.debug(msg)

    # ------------------------------------------------------------------
    # Min / Max open_time (fast using PK index)
    # ------------------------------------------------------------------
    @handle_errors
    def get_min_open_time(self, table_name: str) -> int:
        """Schema-agnostic: handles BIGINT (int ms) and TIMESTAMPTZ (datetime).
        Always returns int Unix-ms — preserves the public contract."""
        query = sql.SQL("SELECT MIN(open_time) FROM {}").format(sql.Identifier(table_name))
        result = self.db_mgr.single_select_query(query)
        if result and result[0] is not None:
            return _open_time_result_to_ms(result[0])
        default = datetime.datetime(2017, 1, 1, tzinfo=timezone.utc)
        return int(default.timestamp() * 1000)

    @handle_errors
    def get_max_open_time(self, table_name: str) -> int:
        """Schema-agnostic: handles BIGINT (int ms) and TIMESTAMPTZ (datetime).
        Always returns int Unix-ms — preserves the public contract."""
        query = sql.SQL("SELECT MAX(open_time) FROM {}").format(sql.Identifier(table_name))
        result = self.db_mgr.single_select_query(query)
        if result and result[0] is not None:
            return _open_time_result_to_ms(result[0])
        now = datetime.datetime.now(timezone.utc)
        return int(now.timestamp() * 1000)

    # ------------------------------------------------------------------
    # Check if table has any data
    # ------------------------------------------------------------------
    @handle_errors
    def is_data_exists(self, table_name: str) -> bool:
        """
        Returns True if the table exists and has at least one row.
        """
        query = sql.SQL("SELECT 1 FROM {} LIMIT 1").format(sql.Identifier(table_name))
        result = self.db_mgr.single_select_query(query)
        return result is not None


class DataRepair:
    def __init__(self, client_cls: Client):
        self.client = client_cls
        self.data_df = pd.DataFrame()

        logger.debug(f"{self.__class__.__name__}: Initializing {self.__class__.__name__}")

        self.open_time_col_name = "open_time"
        self.timeframes_windows: list = []
        self.start_end_close_time: Tuple = (None, None)
        self.ts_start_end_open_time_before: Tuple = (None, None)
        self.ts_start_end_open_time_after: Tuple = (None, None)

    def get_kline_period(self, start_point, end_point, symbol_pair, timeframe) -> List[Any]:
        klines = []
        try:
            klines = self.client.get_historical_klines(symbol_pair,
                                                       timeframe,
                                                       # this datetime format for binance client
                                                       start_point.strftime("%d %b %Y %H:%M:%S"),
                                                       # this datetime format for binance client
                                                       end_point.strftime("%d %b %Y %H:%M:%S"),
                                                       limit=1000
                                                       )
        except BinanceAPIException as error_msg:
            logger.debug(f"{self.__class__.__name__}: exception {error_msg}")
        else:
            pass
        return klines

    @classmethod
    def datetime_round(cls,
                       datetime_series,
                       round_to_interval: str) -> pd.Series:
        if not round_to_interval[-1] == 'W':
            datetime_series = pd.Series(datetime_series).dt.round(round_to_interval)
        else:
            datetime_series = pd.Series(datetime_series).dt.to_period('W-SUN').dt.start_time
        return datetime_series

    @staticmethod
    def get_periods(add_list: list, tm_delta):
        def get_next_window(ts_list):
            ts_list.append(ts_list[-1])
            ts_end = 0
            for idx, (ts, tsplus) in enumerate(zip(ts_list, ts_list[1:])):
                # print(ts, ts + tm_delta, tsplus)
                if (not ts + tm_delta == tsplus) or idx == len(ts_list) - 2:
                    ts_end = idx
                    break
            return ts_end

        windows_list: list = []
        add_list_len = len(add_list)
        ix = 0
        while ix <= add_list_len - 1:
            window_start = ix
            window_end = ix + get_next_window(add_list[ix:])
            window_len = window_end - window_start + 1
            windows_list.append((add_list[window_start], add_list[window_end], window_len))
            ix = window_end + 1
        return windows_list

    @staticmethod
    def convert_int64index_to_timestamp(timestamp_series) -> pd.Series:
        return pd.to_datetime(timestamp_series, unit='ms', )

    @staticmethod
    def convert_timestamp_to_datetime(timestamp_series) -> Union[Timestamp,]:
        dt = pd.to_datetime(timestamp_series, unit='ms')
        dt = pd.to_datetime(dt, format=Constants.default_datetime_format)
        return dt

    @staticmethod
    def convert_datetime_to_timestamp(datetime_series):
        ts = datetime_series.astype('int64') // 10 ** 6
        return ts

    def before_preparation(self, data_df):
        self.ts_start_end_open_time_before = (data_df["open_time"].iloc[0], data_df["open_time"].iloc[-1])

        """ Convert timestamp of 'open_time' to datetime format """
        data_df["open_time"] = self.convert_timestamp_to_datetime(data_df["open_time"])

        """ Set 'open_time' as index """
        data_df.set_index(self.open_time_col_name, inplace=True)

        """ Convert timestamp of 'close_time' to datetime format """
        data_df["close_time"] = self.convert_timestamp_to_datetime(data_df["close_time"])

        """ Remember 1st and last 'close_time' datetime' """
        self.start_end_close_time = (data_df["close_time"].iloc[0], data_df["close_time"].iloc[-1])

        """ Drop "close_time" column to reconstruct back """
        data_df = data_df.drop(columns=["close_time"])

        return data_df

    def after_preparation(self, data_df, timeframe):
        """ TIMESTAMPTZ schema (no 'id'): open_time is the index -
        return it to a column, then convert back to Unix-ms timestamp """
        data_df = data_df.reset_index()
        data_df["open_time"] = self.convert_datetime_to_timestamp(data_df["open_time"])
        self.ts_start_end_open_time_after = (data_df["open_time"].iloc[0], data_df["open_time"].iloc[-1])
        logger.debug(f"{self.__class__.__name__}: Start/end timestamp of 'open_time' before: "
                     f"{self.ts_start_end_open_time_before}")
        logger.debug(f"{self.__class__.__name__}: Start/end timestamp of 'open_time' after: "
                     f"{self.ts_start_end_open_time_after}")

        """ Create new close_time and add it to data_df (position 6: no 'id' column) """
        frequency = convert_timeframe_to_freq(timeframe)
        close_time_dt = pd.date_range(self.start_end_close_time[0], self.start_end_close_time[1], freq=frequency)

        """ Convert _datetime_ of 'close_time' to timestamp format and insert back """
        data_df.insert(6, "close_time", self.convert_datetime_to_timestamp(pd.Series(close_time_dt)))
        return data_df

    @classmethod
    def get_add_delete_sets(cls,
                            check_df: pd.DataFrame,
                            frequency: str,
                            ):
        true_time_intervals = pd.date_range(check_df.index[0], check_df.index[-1], freq=frequency)
        true_time_intervals = cls.datetime_round(true_time_intervals, frequency)
        work_set = set(check_df.index)
        true_set = set(true_time_intervals)
        to_delete_set = work_set.difference(true_set)
        to_add_set = true_set.difference(work_set)
        return to_add_set, to_delete_set

    @classmethod
    def repair_index(cls,
                     to_add_set: set,
                     to_delete_set: set,
                     check_df: pd.DataFrame,
                     open_time_col_name: str = "open_time"
                     ):
        check_df.loc[:, open_time_col_name] = check_df.index
        tmp_df = pd.DataFrame(columns=check_df.columns)
        tmp_df = pd.concat([tmp_df,
                            pd.DataFrame(list(to_add_set), columns=[open_time_col_name])
                            ],
                           axis=0,
                           ignore_index=True)
        check_df = pd.concat([check_df, tmp_df], axis=0, ignore_index=True)

        check_df.reset_index()
        check_df = check_df.sort_values(by=[open_time_col_name])
        check_df.index = check_df[open_time_col_name]
        check_df = check_df.drop(list(to_delete_set))
        check_df = check_df.drop(open_time_col_name, axis=1)
        return check_df, to_add_set, to_delete_set

    def check_and_repair(self,
                         data_df: pd.DataFrame,
                         symbol_pair: str,
                         timeframe: str = '1m',
                         ):
        logger.debug(f"{self.__class__.__name__}: Checking data...")
        data_df = self.before_preparation(data_df)

        frequency = convert_timeframe_to_freq(timeframe)

        to_add_set, to_delete_set = self.get_add_delete_sets(data_df, frequency=frequency)

        if to_add_set or to_delete_set:
            logger.debug(f"'{self.__class__.__name__}': Repairing data...")
            data_df, to_add_timeframes, to_delete_set = self.repair_index(to_add_set,
                                                                          to_delete_set,
                                                                          data_df,
                                                                          open_time_col_name=self.open_time_col_name
                                                                          )

            # data_df, to_add_timeframes, to_delete_set = self.check_add_delete(data_df,
            #                                                                   frequency=frequency,
            #                                                                   open_time_col_name=self.open_time_col_name
            #                                                                   )

            timeframes_list = list(to_add_timeframes)
            timeframes_list.sort()
            delta = pd.to_timedelta(int(timeframe[:-1]), unit=timeframe[-1])
            self.timeframes_windows = self.get_periods(timeframes_list, delta)
            nan_window_start_end: list = [None, None, None]

            loaded_timeframes = 0
            not_loaded_timeframes = 0

            if data_df.isnull().values.any():
                logger.debug(f"{self.__class__.__name__}: Null values founded. Repairing data...")
                total_timeframes = sum([window_data[2] for window_data in self.timeframes_windows])
                logger.info(f"{self.__class__.__name__}: Total missed data windows/timeframes: "
                            f"{len(self.timeframes_windows)}/{total_timeframes}")
                for ix, window_data in enumerate(self.timeframes_windows):
                    logger.debug(f"Window #{ix}: {window_data}")
                    nan_window_start_end[0], nan_window_start_end[1], nan_window_len = window_data

                    kdata = self.get_kline_period(nan_window_start_end[0], nan_window_start_end[1], symbol_pair,
                                                  timeframe)
                    if kdata:
                        temp_df = pd.DataFrame(kdata,
                                               columns=list(Constants.binance_cols),
                                               )
                        # open_time from klines is Unix-ms; make it tz-aware UTC so it
                        # matches the TIMESTAMPTZ-derived (tz-aware) index of data_df.
                        temp_df[self.open_time_col_name] = pd.to_datetime(temp_df[self.open_time_col_name],
                                                                          unit='ms', utc=True)

                        temp_df[self.open_time_col_name] = self.datetime_round(temp_df[self.open_time_col_name],
                                                                               frequency)

                        temp_df = temp_df.drop(columns=["close_time"])
                        """ 
                        Add correct window START and END timeframes to calculate correct offsets
                        """
                        start_flag = False
                        end_flag = False

                        try:
                            pd_infer_freq = pd.infer_freq(pd.DatetimeIndex(temp_df[self.open_time_col_name]))
                        except ValueError:
                            pd_infer_freq = None

                        if not (nan_window_start_end[0] == temp_df[self.open_time_col_name].iloc[0]) and not (
                                pd_infer_freq == 'W-MON'):
                            temp_df = pd.concat(
                                [temp_df, pd.DataFrame([nan_window_start_end[0]], columns=[self.open_time_col_name])],
                                axis=0,
                                ignore_index=True)

                            temp_df = temp_df.sort_values(by=[self.open_time_col_name])
                            start_flag = True
                        try:
                            pd_infer_freq = pd.infer_freq(pd.DatetimeIndex(temp_df[self.open_time_col_name]))
                        except ValueError:
                            pd_infer_freq = None

                        if not (nan_window_start_end[1] == temp_df[self.open_time_col_name].iloc[
                            -1]) and not pd_infer_freq == 'W-MON':
                            temp_df = pd.concat(
                                [temp_df, pd.DataFrame([nan_window_start_end[1]], columns=[self.open_time_col_name])],
                                axis=0,
                                ignore_index=True)
                            # temp_df = temp_df.append(pd.DataFrame([nan_window_start_end[1]], columns=['datetime']),
                            #                          ignore_index=True)
                            end_flag = True
                        temp_df = temp_df.sort_values(by=[self.open_time_col_name])
                        temp_df.set_index(self.open_time_col_name, inplace=True)

                        """
                        Double check df again - what downloaded data is correct
                        """
                        _to_add_set, _to_delete_set = self.get_add_delete_sets(temp_df,
                                                                               frequency=frequency)
                        if _to_add_set or _to_delete_set:
                            # repair_index returns (df, to_add, to_delete) — unpack it,
                            # and use the inner (double-checked) sets, not the outer ones.
                            double_checked_df, _, _ = self.repair_index(_to_add_set, _to_delete_set, temp_df)
                        else:
                            double_checked_df = temp_df

                        data_df_repair_window_start_row = data_df.index.get_loc(double_checked_df.index[0])
                        data_df_repair_window_end_row = data_df.index.get_loc(double_checked_df.index[-1])

                        # Splice the real re-fetched klines in by label, aligning on the
                        # shared index/columns so the repair window is updated in place.
                        data_df.loc[double_checked_df.index, double_checked_df.columns] = double_checked_df

                        current_not_loaded = int(
                            data_df.iloc[data_df_repair_window_start_row:data_df_repair_window_end_row,
                            1].isnull().values.sum())
                        not_loaded_timeframes += current_not_loaded
                        loaded_timeframes += double_checked_df.shape[0] - current_not_loaded

                        """ Repair only empty windows data with .rolling().mean() """
                        if start_flag:
                            _to_add_set.add(nan_window_start_end[0])
                        if end_flag:
                            _to_add_set.add(nan_window_start_end[1])
                        tm_list = list(_to_add_set)
                        tm_list.sort()

                        empty_windows = self.get_periods(tm_list, delta)
                        empty_window_start_end: list = [None, None, None]
                        for empty_window_data in empty_windows:
                            empty_window_start_end[0], empty_window_start_end[1], empty_window_len = empty_window_data

                            repair_window_start_row = data_df.index.get_loc(
                                empty_window_start_end[0]) - empty_window_len
                            repair_window_end_row = data_df.index.get_loc(empty_window_start_end[1]) + 2
                            buffer_df = data_df.iloc[repair_window_start_row:repair_window_end_row, :].fillna(
                                data_df.iloc[repair_window_start_row:repair_window_end_row, :].rolling(
                                    empty_window_len + 1,
                                    min_periods=1).mean())
                            data_df.iloc[repair_window_start_row:repair_window_end_row, :] = buffer_df
                    else:
                        """
                        If kdata is empty, we already have this rows empty in data_df
                        Repair empty_window data with .rolling().mean()
                        """
                        repair_window_start_row = data_df.index.get_loc(nan_window_start_end[0]) - nan_window_len
                        repair_window_end_row = data_df.index.get_loc(nan_window_start_end[1]) + 2
                        buffer_df = data_df.iloc[repair_window_start_row:repair_window_end_row, :].fillna(
                            data_df.iloc[repair_window_start_row:repair_window_end_row, :].rolling(
                                nan_window_len + 1,
                                min_periods=1).mean())
                        data_df.iloc[repair_window_start_row:repair_window_end_row, :] = buffer_df

                        not_loaded_timeframes += nan_window_len
                    # msg = f'\rTotal/loaded/NOT loaded timeframes: ' \
                    #       f'{total_timeframes}/{loaded_timeframes}/{not_loaded_timeframes}'
                    # print(msg, end='')
                logger.info(
                    f"{self.__class__.__name__}: Total/loaded/NOT loaded timeframes: "
                    f"{total_timeframes}/{loaded_timeframes}/{not_loaded_timeframes}")

                """ Reconstruct open_time/close_time columns into newsql_cols order """
                data_df = self.after_preparation(data_df, timeframe)

                for col_name in data_df.columns:
                    dict_name = col_name
                    if col_name[0].isupper():
                        dict_name = dict_name[0].lower()
                    col_dtype = Constants.newsql_dtypes.get(dict_name, None)
                    if col_name in ("open_time", "close_time"):
                        # after_preparation emits these as Unix-ms ints (the format
                        # insert_klines_to_table consumes). Casting to datetime64[ns]
                        # here would reinterpret the ms value as ns and corrupt it.
                        col_dtype = "int64"
                    data_df[col_name] = data_df[col_name].astype(col_dtype)
        else:
            logger.debug(f"{self.__class__.__name__}: Data is ok. Repairing skipped...")
        return data_df


updater_status: bool = False


class DataUpdaterMeta(PostgreSQLDatabase):
    updater_is_running = updater_status

    def __init__(self, host, database, user, password, binance_api_key, binance_api_secret,
                 symbol_pairs=None, timeframes=None):

        super().__init__(host, database, user, password)
        # logger.debug from parent class

        if symbol_pairs is None:
            self.symbol_pairs: list = ['BTCUSDT']
        else:
            self.symbol_pairs = symbol_pairs
        if timeframes is None:
            self.timeframes: list = ['1m']
        else:
            self.timeframes = timeframes

        if binance_api_key.lower() != "dummy" or binance_api_secret.lower() != "dummy":
            """ Connect to Binance """
            self.client = Client(api_key=binance_api_key, api_secret=binance_api_secret)

            """ Connect repair class"""
            self.datarepair = DataRepair(client_cls=self.client)
        else:
            logger.info(f"{self.__class__.__name__} #{self.idnum}: running in dummy mode w/o connection to binance")

        self.base_tables_names = Constants.base_tables_names

        self.default_agg_dict = {'open_time': 'first', 'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last',
                                 'volume': 'sum', 'close_time': 'last', 'quote_asset_volume': 'sum',
                                 # short names matching the actual DB / Constants.binance_cols columns
                                 # (the long-name keys below stayed unused -> extended resample got
                                 # agg=None for these and broke; keep both for back-compat).
                                 'trades': 'sum', 'taker_buy_base': 'sum', 'taker_buy_quote': 'sum',
                                 'number_of_trades': 'sum', 'taker_buy_base_asset_volume': 'sum',
                                 'taker_buy_quote_asset_volume': 'sum'}

        self.back_scheduler = None
        self.last_timeframe_datetime = None

    def prepare_start_end(self,
                          table_name: str,
                          start: Optional[Union[datetime.datetime, int, str]],
                          end: Optional[Union[datetime.datetime, int, str]]
                          ) -> Tuple[Any, Any]:
        """Normalize start/end and type-match them to the open_time column.

        BIGINT      column → Tuple[int, int]            (Unix milliseconds)
        TIMESTAMPTZ column → Tuple[datetime, datetime]  (UTC-aware)

        Matching the return type to the column type lets sql.Literal()
        render a WHERE clause that the column actually accepts. Mixed-type
        comparison (int literal vs TIMESTAMPTZ column, or vice versa) is
        rejected by Postgres at parse time.
        """

        # -----------------------------
        # Convert start to datetime UTC (intermediate representation)
        # -----------------------------
        if isinstance(start, int):
            # assume timestamp in milliseconds
            start_dt = datetime.datetime.fromtimestamp(start / 1000, tz=timezone.utc)
        elif isinstance(start, str):
            start_dt = datetime.datetime.strptime(start, Constants.default_datetime_format).replace(tzinfo=timezone.utc)
        elif isinstance(start, datetime.datetime):
            # force UTC
            if start.tzinfo is None:
                start_dt = start.replace(tzinfo=timezone.utc)
            else:
                start_dt = start.astimezone(timezone.utc)
        else:
            # None → use table min time
            start_dt = datetime.datetime.fromtimestamp(self.get_min_open_time(table_name) / 1000, tz=timezone.utc)

        # -----------------------------
        # Convert end to datetime UTC (intermediate representation)
        # -----------------------------
        if end is None:
            end_dt = datetime.datetime.fromtimestamp(self.get_max_open_time(table_name) / 1000, tz=timezone.utc)
        elif isinstance(end, int):
            end_dt = datetime.datetime.fromtimestamp(end / 1000, tz=timezone.utc)
        elif isinstance(end, str):
            end_dt = datetime.datetime.strptime(end, Constants.default_datetime_format).replace(tzinfo=timezone.utc)
        else:
            if end.tzinfo is None:
                end_dt = end.replace(tzinfo=timezone.utc)
            else:
                end_dt = end.astimezone(timezone.utc)

        # -----------------------------
        # Ensure start >= min_open_time
        # -----------------------------
        min_open_time = datetime.datetime.fromtimestamp(self.get_min_open_time(table_name) / 1000, tz=timezone.utc)
        if start_dt < min_open_time:
            start_dt = min_open_time

        # -----------------------------
        # Type-match to column schema
        # -----------------------------
        col_type = _SchemaCache.get(self.db_mgr, table_name)
        if col_type == BIGINT:
            return (int(start_dt.timestamp() * 1000),
                    int(end_dt.timestamp() * 1000))
        return start_dt, end_dt

    # def prepare_start_end(self,
    #                       table_name: str,
    #                       start: Optional[Union[datetime.datetime, int, str]],
    #                       end: Optional[Union[datetime.datetime, int, str]]) -> Tuple[int, int]:
    #
    #     if isinstance(start, int):
    #         start_timestamp = int(start)
    #     elif isinstance(start, str):
    #         start_timestamp = datetime.datetime.strptime(start,
    #                                                      Constants.default_datetime_format).replace(tzinfo=pytz.utc)
    #         start_timestamp = int(start_timestamp.timestamp() * 1000)
    #     else:
    #         start_timestamp = int(start.timestamp() * 1000)
    #
    #     if end is None:
    #         end_timestamp = int(self.get_max_open_time(table_name))
    #     elif isinstance(end, int):
    #         end_timestamp = int(end)
    #     elif isinstance(end, str):
    #         end_timestamp = datetime.datetime.strptime(end,
    #                                                    Constants.default_datetime_format).replace(tzinfo=pytz.utc)
    #         end_timestamp = int(end_timestamp.timestamp() * 1000)
    #     else:
    #         end_timestamp = int(end.timestamp() * 1000)
    #
    #     start_open_time = int(self.get_min_open_time(table_name))
    #
    #     if start_timestamp < start_open_time or start_timestamp is None:
    #         start_timestamp = start_open_time
    #
    #     return start_timestamp, end_timestamp


class DataUpdater(DataUpdaterMeta):
    updater_is_running = updater_status

    def __init__(self, host, database, user, password, binance_api_key, binance_api_secret,
                 symbol_pairs=None, timeframes=None):
        super().__init__(host, database, user, password, binance_api_key, binance_api_secret, symbol_pairs, timeframes)

    def check_first_run(self):
        """ Check if this a first run """
        msg = f"{self.__class__.__name__} #{self.idnum}: Checking the 1st run"
        logger.info(msg)
        for base_table_name in self.base_tables_names:
            for symbol_pair in self.symbol_pairs:
                for timeframe in self.timeframes:
                    table_name = f"{base_table_name}_{symbol_pair}_{timeframe}".lower()
                    if not self.is_table_exists(table_name):
                        msg = f"{self.__class__.__name__} #{self.idnum}: The table '{table_name}' is not exists. " \
                              f"Creating new table '{table_name}'"
                        logger.info(msg)
                        self.create_table(table_name)
                        # self.is_duplicates_exists(table_name)
                        self.fetch_historical_spot_data(base_table_name, symbol_pair, timeframe)
                        self.check_and_repair(table_name, symbol_pair, timeframe)
                        continue
                    else:
                        if not self.is_data_exists(table_name):
                            msg = f"{self.__class__.__name__} #{self.idnum}:The table '{table_name}' is present. " \
                                  f"But data is empty. Fetching data for symbol pair: {symbol_pair}"
                            logger.info(msg)
                            self.fetch_historical_spot_data(base_table_name, symbol_pair, timeframe)
                            self.check_and_repair(table_name, symbol_pair, timeframe)
                            continue
                        else:
                            msg = f"{self.__class__.__name__} #{self.idnum}: Updating database to " \
                                  f"actual {symbol_pair}, {timeframe}"
                            logger.debug(msg)
                            self.check_and_repair(table_name, symbol_pair, timeframe)
                            self.update_spot_data()

    def fetch_historical_spot_data(self, base_table_name, symbol_pair, timeframe):
        table_name = f"{base_table_name}_{symbol_pair}_{timeframe}".lower()
        start_time = datetime.datetime.now(timezone.utc)
        logger.debug(f"{self.__class__.__name__} #{self.idnum}: Start fetching historical data for "
                     f"{symbol_pair}, {timeframe}: {start_time}")
        klines = []
        try:
            klines = self.client.get_historical_klines(symbol=symbol_pair,
                                                       interval=timeframe,
                                                       start_str="01 Jan 2017",
                                                       limit=1000)
        except BinanceAPIException as error_msg:
            logger.debug(f'Exception {error_msg}')
        else:
            if klines:
                logger.debug(f"Timeframes: {len(klines)}, ETA: {datetime.datetime.now(timezone.utc) - start_time} "
                             f"start: {datetime.datetime.fromtimestamp(klines[0][0] / 1000, tz=pytz.utc)}, "
                             f"end: {datetime.datetime.fromtimestamp(klines[-1][0] / 1000, tz=pytz.utc)}")

        start_time = datetime.datetime.now(timezone.utc)
        logger.debug(f"{self.__class__.__name__} #{self.idnum}: Start saving historical data to database for "
                     f"{symbol_pair}, {timeframe} at: {start_time}")

        self.insert_klines_to_table(table_name, klines)

        logger.debug(f"{self.__class__.__name__} #{self.idnum}: Finished saving historical data to database for "
                     f"{symbol_pair}, {timeframe} at: {datetime.datetime.now(timezone.utc)}. "
                     f"ETA: {datetime.datetime.now(timezone.utc) - start_time}")

    @staticmethod
    def convert_timeframe_to_min(timeframe):
        bin_size = Constants.binsizes[f'1{timeframe.lower()[-1:]}']
        # if timeframe[-1:] == 'h' or timeframe[-1:] == 'H':
        #     min_size = 60
        # elif timeframe[-1:] == 'm':
        #     min_size = 1
        # elif timeframe[-1:] == 'd' or timeframe[-1:] == 'D':
        #     min_size = 24 * 60
        # elif timeframe[-1:] == 'w' or timeframe[-1:] == 'W':
        #     min_size = 7 * 24 * 60
        # converted_data = min_size * int(timeframe[:-1])
        converted_data = bin_size * int(timeframe[:-1])
        return converted_data

    def update_spot_data(self):
        logger.debug(f"{self.__class__.__name__} #{self.idnum}: Update started...")
        # self.cur = self.conn.cursor()
        for base_table_name in self.base_tables_names:
            for symbol_pair in self.symbol_pairs:
                for timeframe in self.timeframes:
                    table_name = f"{base_table_name}_{symbol_pair}_{timeframe}".lower()
                    start_open_time = self.get_max_open_time(table_name)
                    start_open_time = datetime.datetime.fromtimestamp(start_open_time / 1000, tz=pytz.utc)
                    # B1 (tail overlap): start LIVE_OVERLAP_BARS before max_open_time (instead of
                    # +1 bar) so the last closed bars are re-read each run and any partial bar that
                    # slipped through earlier is re-fetched closed and corrected by the upsert.
                    start_open_time = start_open_time - datetime.timedelta(
                        minutes=self.convert_timeframe_to_min(timeframe) * LIVE_OVERLAP_BARS)
                    until_open_time = start_open_time + datetime.timedelta(days=365 * 5)

                    start_open_time = start_open_time.strftime("%d %b %Y %H:%M:%S")
                    until_open_time = until_open_time.strftime("%d %b %Y %H:%M:%S")

                    logger.debug(f"{self.__class__.__name__} #{self.idnum}: "
                                 f"table: {table_name}, {start_open_time} - {until_open_time} ")

                    # start_time = datetime.datetime.now(timezone.utc)
                    try:
                        klines = self.client.get_historical_klines(symbol_pair,
                                                                   timeframe,
                                                                   start_open_time,
                                                                   until_open_time,
                                                                   limit=1000)
                    except BinanceAPIException as error_msg:
                        logger.debug(f"{self.__class__.__name__} #{self.idnum}: Updater - exception {error_msg}")
                    else:
                        # A: drop the still-forming candle before persisting / logging.
                        klines = drop_unclosed_klines(klines)
                        if klines:
                            self.last_timeframe_datetime = datetime.datetime.fromtimestamp(
                                klines[-1][0] / 1000, tz=pytz.utc)
                            logger.info(
                                f"{self.__class__.__name__} #{self.idnum}: Updater - {table_name} - "
                                f"timeframes: {len(klines)}. Last timeframe: {self.last_timeframe_datetime}")
                            self.insert_klines_to_table(table_name, klines)

                        logger.debug(f"{self.__class__.__name__} #{self.idnum}: Updating '{table_name}' finished...")
                    # for kline in klines:
                    #     self.insert_kline_to_table(table_name, kline)
        logger.debug(f"{self.__class__.__name__} #{self.idnum}: Update of all symbols finished...")

    # @handle_errors
    def get_data_as_df(self,
                       table_name: str,
                       start: Union[datetime.datetime, int],
                       end: Union[datetime.datetime, int],
                       use_cols=Constants.newsql_cols,
                       use_dtypes=None,
                       ) -> pd.DataFrame:

        _start_time = datetime.datetime.now(timezone.utc)
        if use_dtypes is None:
            use_dtypes = Constants.newsql_dtypes

        start_timestamp, end_timestamp = self.prepare_start_end(table_name, start, end)
        query = sql.SQL("""
            SELECT {cols}
            FROM {table}
            WHERE open_time BETWEEN {start_ts} AND {end_ts}
            ORDER BY open_time
        """).format(
            cols=sql.SQL(', ').join(map(sql.Identifier, use_cols)),  # Join column names safely
            table=sql.Identifier(table_name),  # Protect against SQL injection
            start_ts=sql.Literal(start_timestamp),  # Escape timestamps
            end_ts=sql.Literal(end_timestamp)  # Escape timestamps
        )
        logger.debug(f"{self.__class__.__name__} #{self.idnum}: "
                     f"Start getting historical data from database table {table_name} at: {_start_time}")
        data = self.db_mgr.select_query(query)

        logger.debug(f"{self.__class__.__name__} #{self.idnum}: "
                     f"Finished getting historical data from database table {table_name} at: "
                     f"{datetime.datetime.now(timezone.utc)}. "
                     f"ETA: {datetime.datetime.now(timezone.utc) - _start_time}")

        data_df = pd.DataFrame(data, columns=list(use_cols))
        data_df = data_df.astype(use_dtypes)
        return data_df

    def is_symbol_pair_available(self, market, symbol_pair) -> bool:
        base_table_name = f"{market}_data"
        timeframe = "1m"
        table_name = f"{base_table_name}_{symbol_pair}_{timeframe}".lower()
        status = self.is_table_exists(table_name)
        return status

    def get_all_data_as_df(self, table_name, use_cols=Constants.newsql_cols, use_dtypes=None) -> pd.DataFrame:
        if use_dtypes is None:
            use_dtypes = Constants.newsql_dtypes

        start_open_time = self.get_min_open_time(table_name)
        end_open_time = self.get_max_open_time(table_name)
        data_df = self.get_data_as_df(table_name, start_open_time, end_open_time, use_cols, use_dtypes)
        return data_df

    def check_and_repair(self, table_name, symbol_pair, timeframe):
        start_time = datetime.datetime.now(timezone.utc)
        logger.debug(f"{self.__class__.__name__} #{self.idnum}: "
                     f"Check & repair {table_name}. "
                     f"Loading data. ETA: {datetime.datetime.now(timezone.utc) - start_time}")
        data_df = self.get_all_data_as_df(table_name)

        start_time = datetime.datetime.now(timezone.utc)
        logger.debug(f"{self.__class__.__name__} #{self.idnum}: "
                     f"Check & repair {table_name}. Start: {start_time}")
        data_df = self.datarepair.check_and_repair(data_df, symbol_pair, timeframe)
        logger.debug(f"{self.__class__.__name__} #{self.idnum}: "
                     f"Check & repair. Finished. ETA: {datetime.datetime.now(timezone.utc) - start_time}")

        if self.datarepair.timeframes_windows:
            start_time = datetime.datetime.now(timezone.utc)
            logger.info(f"{self.__class__.__name__} #{self.idnum}: "
                        f"'{table_name}' - Database repaired. Saving repaired data back to database.")
            logger.debug(f"{self.__class__.__name__} #{self.idnum}: "
                         f"'{table_name}' - Start saving repaired data back to database: {start_time}")
            # read/repair path uses newsql_cols (no 'id'); insert directly
            self.insert_klines_to_table(table_name, data_df.values.tolist())
            logger.debug(f"{self.__class__.__name__} #{self.idnum}: "
                         f"'{table_name}' - data saved. ETA: {datetime.datetime.now(timezone.utc) - start_time}")
        else:
            logger.info(f"{self.__class__.__name__} #{self.idnum}: "
                        f"'{table_name}' - Database already repaired. Don't need any action.")

    def start_background_updater(self,
                                 update_interval: int = 1,  # in minutes
                                 ):
        # Create a new background scheduler
        if not DataUpdater.updater_is_running:
            self.back_scheduler = BackgroundScheduler()

            """ if updater is not running, check and repair database before we run updater """
            self.check_first_run()

            # Start a few seconds after the minute boundary so the just-closed bar is finalized
            # on Binance (clock skew / late-reported trades); the unclosed bar is dropped anyway (A).
            back_start_time = ceil_time(datetime.datetime.now(), "1m").replace(second=5, microsecond=0)
            # Add a new job to the scheduler to run the update_database method every minute
            self.back_scheduler.add_job(self.update_spot_data,
                                        'interval',
                                        minutes=update_interval,
                                        start_date=back_start_time,
                                        )

            # Start the scheduler
            self.back_scheduler.start()
            DataUpdater.updater_is_running = True

    def test_background_updater(self, sleep_time: int = 60):

        self.start_background_updater(update_interval=int(sleep_time // 60))

        print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))
        try:
            # This is here to simulate application activity (which keeps the main thread alive).
            while True:
                time.sleep(sleep_time)
        except (KeyboardInterrupt, SystemExit):
            # Not strictly necessary if daemonic mode is enabled but should be done if possible
            DataUpdater.updater_is_running = False
            self.back_scheduler.shutdown()


cache_manager_obj = FetcherCacheManager(max_memory_gb=2)


class DataFetcher(DataUpdaterMeta):
    CM = cache_manager_obj

    def __init__(self, host, database, user, password, binance_api_key, binance_api_secret,
                 cache_obj: Optional[FetcherCacheManager] = None):
        if cache_obj is not None:
            self.set_new_cache_obj(cache_obj)
        super().__init__(host, database, user, password, binance_api_key, binance_api_secret)

    @classmethod
    def set_new_cache_obj(cls, cache_obj):
        cls.CM = cache_obj

    def _get_agg_dict(self, use_cols):
        actual_agg_dict = OrderedDict()
        for col_name in use_cols:
            if col_name != "open_time":
                actual_agg_dict.update({col_name: self.default_agg_dict.get(col_name, None)})
        return actual_agg_dict

    @staticmethod
    def _resolve_use_cols(use_cols, use_extended_cols: bool):
        """Pick the columns to resample: extended kline set vs the caller's ``use_cols``.

        Helper shared by the (pg_)resample_to_timeframe methods. When ``use_extended_cols`` is
        True, returns ``Constants.binance_extended_cols`` (OHLCV + quote_asset_volume + trades +
        taker_buy_base + taker_buy_quote — the order-flow / microstructure fields), overriding
        ``use_cols``. When False, returns ``use_cols`` unchanged (back-compatible default).

        Args:
            use_cols: columns the caller passed.
            use_extended_cols: select the extended kline set instead.

        Returns:
            The tuple of columns to resample.
        """
        return Constants.binance_extended_cols if use_extended_cols else use_cols

    @staticmethod
    def _timeframe_to_ms(to_timeframe: str) -> int:
        return get_timeframe_bins(to_timeframe) * 60_000

    @staticmethod
    def _drop_partial_tail(df, freq_ms: int, end_timestamp, time_in_index: bool):
        """Drop trailing bin(s) whose ``[label, label+interval)`` window extends past ``end``
        — an INCOMPLETE last bar (only partial sub-period data is available up to ``end``).

        Keeps complete bars only, matching Binance native klines (which never return an
        in-progress candle for a closed range). ``time_in_index`` is True when the bin label is
        the DataFrame index, False when it is the ``open_time`` column. A complete bin satisfies
        ``label + interval <= end``.
        """
        if df is None or len(df) == 0:
            return df
        if isinstance(end_timestamp, datetime.datetime):
            end_ts = pd.to_datetime(end_timestamp, utc=True)
        else:
            end_ts = pd.to_datetime(int(end_timestamp), unit='ms', utc=True)
        freq = pd.Timedelta(milliseconds=freq_ms)
        labels = df.index if time_in_index else pd.to_datetime(df['open_time'], utc=True)
        return df[(labels + freq) <= end_ts]

    def _build_pg_resample_query(
            self,
            col_type: str,
            table_name: str,
            freq_ms: int,
            use_cols: tuple,
            agg_dict: dict,
            start_timestamp,
            end_timestamp,
    ):
        _AGG_SQL = {
            'first': 'MAX(CASE WHEN rn_asc=1 THEN {col} END)',
            'last': 'MAX(CASE WHEN rn_desc=1 THEN {col} END)',
            'max': 'MAX({col})',
            'min': 'MIN({col})',
            'sum': 'SUM({col})',
        }

        data_cols = ', '.join(c for c in use_cols if c != 'open_time')

        _one_day_ms = 86_400_000
        if freq_ms > _one_day_ms:
            # Multi-day (nD, n>1): pandas uses calendar-day granularity.
            # Formula: bin = floor((delta + freq - 1D) / freq)
            _offset = freq_ms - _one_day_ms
            if col_type == BIGINT:
                bin_formula_tpl = (
                    f"{{start_ts}}::bigint + ((open_time - {{start_ts}}::bigint + {_offset}) / {freq_ms}) * {freq_ms}"
                )
            else:
                bin_formula_tpl = (
                    f"{{start_ts}}::timestamptz + FLOOR((EXTRACT(EPOCH FROM (open_time - {{start_ts}}::timestamptz)) "
                    f"* 1000.0 + {_offset}.0) / {freq_ms})::bigint "
                    f"* interval '{freq_ms} milliseconds'"
                )
        else:
            # Sub-day or exactly 1 day: FLOOR(delta / freq) labels each bar by its START,
            # closed='left' -> bar [T, T+freq) == Binance native kline (openTime=bin start,
            # closeTime=T+freq-1ms). Verified vs Binance API (30m @T open == 1m @T open).
            if col_type == BIGINT:
                bin_formula_tpl = (
                    f"{{start_ts}}::bigint + ((open_time - {{start_ts}}::bigint) / {freq_ms}) * {freq_ms}"
                )
            else:
                bin_formula_tpl = (
                    f"{{start_ts}}::timestamptz + (FLOOR(EXTRACT(EPOCH FROM (open_time - {{start_ts}}::timestamptz)) "
                    f"* 1000.0 / {freq_ms}))::bigint * interval '{freq_ms} milliseconds'"
                )

        agg_exprs = ',\n    '.join(
            _AGG_SQL[fn].format(col=col)
            for col, fn in agg_dict.items()
        )

        query_template = f"""
WITH pre AS (
    SELECT
        {bin_formula_tpl} AS bin_label,
        open_time,
        {data_cols}
    FROM {{table_name}}
    WHERE open_time >= {{start_ts}} AND open_time <= {{end_ts}}
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

        return sql.SQL(query_template).format(
            table_name=sql.Identifier(table_name),
            start_ts=sql.Literal(start_timestamp),
            end_ts=sql.Literal(end_timestamp),
        )

    def resample_to_timeframe(self,
                              table_name: str,
                              start: Union[datetime.datetime, int],
                              end: Union[datetime.datetime, int],
                              to_timeframe: str = "1h",
                              origin: str = "start",
                              use_cols: tuple = Constants.binance_cols,
                              use_dtypes=None,
                              open_time_index: bool = True,
                              cached=False,
                              last_full_bar: bool = True,
                              use_extended_cols: bool = False,
                              ) -> pd.DataFrame:
        use_cols = self._resolve_use_cols(use_cols, use_extended_cols)

        def fetch_raw_data():
            query = sql.SQL("""
                SELECT {cols}
                FROM {table}
                WHERE open_time >= {start_ts} AND open_time <= {end_ts};
            """).format(
                cols=sql.SQL(', ').join(map(sql.Identifier, use_cols)),
                table=sql.Identifier(table_name),
                start_ts=sql.Literal(start_timestamp),
                end_ts=sql.Literal(end_timestamp)
            )

            with ThreadPool(self.pool) as conn:
                # conn.set_session(readonly=True, autocommit=True)
                with conn.cursor() as cur:
                    cur.execute(query)
                    raw_data = cur.fetchall()
                return raw_data

        # Resolve column schema once — used by prepare_raw_df / prepare_resampled_df.
        _col_type = _SchemaCache.get(self.db_mgr, table_name)
        _ot_unit = 'ms' if _col_type == BIGINT else 'ns'

        def prepare_raw_df():
            _df = pd.DataFrame(fetch_raw_data(), columns=list(use_cols))
            _df = _df.astype(use_dtypes)
            # Stage D / TIMESTAMPTZ schema: caller may pass use_dtypes
            # with 'open_time': int (legacy ohlcv_dtypes), which casts
            # tz-aware datetime → int64-ns and breaks downstream
            # .loc[Timestamp:Timestamp] cache subset. Restore native
            # datetime64[ns, UTC] so the cache index stays Timestamp-typed
            # end-to-end (matches the TIMESTAMPTZ column).
            if _col_type == TIMESTAMPTZ:
                _df['open_time'] = pd.to_datetime(_df['open_time'], unit='ns', utc=True)
            _df = _df.sort_values(by=['open_time']).set_index('open_time', inplace=False, drop=False)
            return _df

        def get_raw_df():
            raw_df = None
            """ Cached raw df (minutes) """
            if cached:
                cache_key = self.CM.get_cache_key(table_name=table_name, start_timestamp=start_timestamp,
                                                  end_timestamp=end_timestamp,
                                                  use_extended_cols=use_extended_cols)

                #   check if data is already in
                if cache_key in self.CM.cache.keys():
                    raw_df = self.CM.cache[cache_key]
                    logger.debug(f"{self.__class__.__name__} #{self.idnum}: Return cached RAW data: {table_name} / "
                                 f"{start_timestamp} - {end_timestamp})")
                else:
                    for key in self.CM.cache.keys():
                        if (len(key) == 3) and (key[2][1] == table_name) and (start_timestamp >= key[1][1]) and (
                                end_timestamp <= key[0][1]):
                            raw_df = self.CM.cache[key].loc[start_timestamp:end_timestamp]
                            logger.debug(
                                f"{self.__class__.__name__} #{self.idnum}: Return cached RAW data: {table_name} / "
                                f"{start_timestamp} - {end_timestamp})")
                            break
                    if raw_df is None:
                        raw_df = prepare_raw_df()
                        self.CM.update_cache(key=cache_key, value=raw_df.copy(deep=True))
            else:
                raw_df = prepare_raw_df()
            return raw_df.copy(deep=True)

        def prepare_resampled_df():
            resampled_df = get_raw_df()

            # BIGINT path: column is int(ms) after astype, convert to datetime.
            # TIMESTAMPTZ path: already datetime64[ns, UTC] (see prepare_raw_df).
            if _col_type == BIGINT:
                resampled_df['open_time'] = pd.to_datetime(resampled_df['open_time'],
                                                           unit=_ot_unit,
                                                           utc=True,
                                                           )
            resampled_df = resampled_df.set_index('open_time', inplace=False)

            agg_dict = self._get_agg_dict(use_cols)
            freq = convert_timeframe_to_freq(to_timeframe)

            # label='left', closed='left' -> bar [T, T+freq) labeled by its START == Binance
            # native kline convention (openTime = bin start). Verified vs Binance API.
            resampled_df = resampled_df.resample(freq, label='left', closed='left', origin=origin).agg(agg_dict)
            if last_full_bar:
                resampled_df = self._drop_partial_tail(
                    resampled_df, self._timeframe_to_ms(to_timeframe), end_timestamp, time_in_index=True)
            if not open_time_index:
                resampled_df = resampled_df.reset_index()

            logger.debug(f"{self.__class__.__name__} #{self.idnum}: "
                         f"'{table_name}' resample to timeframe: '{freq}', "
                         f"from origin: {origin}, start-end: "
                         f"{start_timestamp} - {end_timestamp}")
            return resampled_df

        def get_resampled_df():
            if cached:
                cache_key = self.CM.get_cache_key(table_name=table_name, start_timestamp=start_timestamp,
                                                  end_timestamp=end_timestamp, timeframe=to_timeframe, origin=origin,
                                                  open_time_index=open_time_index, last_full_bar=last_full_bar,
                                                  use_extended_cols=use_extended_cols)

                if cache_key in self.CM.cache.keys():
                    resampled_df = self.CM.cache[cache_key]
                    msg = (
                        f"{self.__class__.__name__} #{self.idnum}: Return cached RESAMPLED data: {table_name} / "
                        f"{start_timestamp}) - {end_timestamp})")
                    logger.debug(msg)
                else:
                    resampled_df = prepare_resampled_df()
                    self.CM.update_cache(key=cache_key, value=resampled_df.copy(deep=True))
            else:
                resampled_df = prepare_resampled_df()

            return resampled_df

        if use_dtypes is None:
            use_dtypes = Constants.binance_dtypes

        start_timestamp, end_timestamp = self.prepare_start_end(table_name, start, end)

        data_df = get_resampled_df()
        return data_df.copy(deep=True)

    def pg_resample_to_timeframe(
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
            last_full_bar: bool = True,
            use_extended_cols: bool = False,
    ) -> pd.DataFrame:
        use_cols = self._resolve_use_cols(use_cols, use_extended_cols)
        start_timestamp, end_timestamp = self.prepare_start_end(table_name, start, end)
        col_type = _SchemaCache.get(self.db_mgr, table_name)
        freq_ms = self._timeframe_to_ms(to_timeframe)
        agg_dict = self._get_agg_dict(use_cols)

        def fetch_and_build() -> pd.DataFrame:
            query = self._build_pg_resample_query(
                col_type=col_type,
                table_name=table_name,
                freq_ms=freq_ms,
                use_cols=use_cols,
                agg_dict=agg_dict,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
            )
            with ThreadPool(self.pool) as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    records = cur.fetchall()

            out_cols = ['open_time'] + [c for c in use_cols if c != 'open_time']
            df = pd.DataFrame(records, columns=out_cols)

            if use_dtypes:
                df = df.astype({c: use_dtypes[c] for c in use_dtypes if c in df.columns and c != 'open_time'})

            if col_type == BIGINT:
                df['open_time'] = pd.to_datetime(df['open_time'], unit='ms', utc=True)
            else:
                df['open_time'] = pd.to_datetime(df['open_time'], utc=True)

            if last_full_bar:
                df = self._drop_partial_tail(df, freq_ms, end_timestamp, time_in_index=False)

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
                last_full_bar=last_full_bar,
                use_extended_cols=use_extended_cols,
                method='pg',
            )
            if cache_key in self.CM.cache:
                logger.debug(
                    f"{self.__class__.__name__} #{self.idnum}: "
                    f"Return cached PG RESAMPLED data: {table_name} / "
                    f"{start_timestamp} - {end_timestamp}"
                )
                return self.CM.cache[cache_key].copy(deep=True)

            df = fetch_and_build()
            self.CM.update_cache(key=cache_key, value=df.copy(deep=True))
            return df.copy(deep=True)

        return fetch_and_build()


if __name__ == "__main__":
    from dbbinance.config.configpostgresql import ConfigPostgreSQL
    from dbbinance.config.configbinance import ConfigBinance

    # from secureapikey.secureaes import Secure

    logger.setLevel(logging.DEBUG)

    file_handler = logging.FileHandler('datafetcher.log')
    file_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    logging.getLogger('apscheduler').setLevel(logging.DEBUG)
    """
    Various Checks
    """

    """ Decrypt binance api key and binance api secret """
    # secure_key = Secure()
    # _binance_api_key, _binance_api_secret = secure_key.get_key('BINANCE', use_env_salt=True)

    updater = DataUpdater(host=ConfigPostgreSQL.HOST,
                          database=ConfigPostgreSQL.DATABASE,
                          user=ConfigPostgreSQL.USER,
                          password=ConfigPostgreSQL.PASSWORD,
                          binance_api_key=ConfigBinance.BINANCE_API_SECRET,
                          binance_api_secret=ConfigBinance.BINANCE_API_SECRET,
                          )
    # updater.test_background_updater()
    # updater.drop_duplicates_rows(table_name="spot_data_btcusdt_1m")

    """ Start testing get_data_as_df method """
    start_datetime = datetime.datetime.strptime('01 Aug 2017', '%d %b %Y').replace(tzinfo=pytz.utc)
    _data_df = updater.get_data_as_df(table_name="spot_data_btcusdt_1m",
                                      start=start_datetime,
                                      end=datetime.datetime.now(timezone.utc),
                                      use_cols=Constants.ohlcv_cols,
                                      use_dtypes=Constants.ohlcv_dtypes
                                      )
    # _data_df['open_time'] = pd.to_datetime(_data_df['open_time'], unit='ms')
    # _data_df['open_time'] = pd.to_datetime(_data_df['open_time'],
    #                                        infer_datetime_format=True,
    #                                        format=Constants.default_datetime_format)

    # _data_df['open_time'] = DataRepair.convert_timestamp_to_datetime(_data_df['open_time'])
    _data_df['open_time'] = pd.to_datetime(_data_df['open_time'], unit='ns', infer_datetime_format=True, utc=True, )
    # _data_df['open_time'] = DataRepair.convert_int64index_to_timestamp(_data_df['open_time']/1000)
    # df['datetime_utc'] = pd.to_datetime(df['ns_time'], unit='ns', utc=True)
    _data_df = _data_df.set_index('open_time', inplace=False)

    print(_data_df.head(15).to_string())
    print(_data_df.tail(15).to_string())
    """ End testing get_data_as_df method """

    """ Start testing resample_to_timeframe method and 'cached' option"""
    fetcher = DataFetcher(host=ConfigPostgreSQL.HOST,
                          database=ConfigPostgreSQL.DATABASE,
                          user=ConfigPostgreSQL.USER,
                          password=ConfigPostgreSQL.PASSWORD,
                          binance_api_key='dummy',
                          binance_api_secret='dummy',
                          )

    print("\nResampling of same period and compare results with 'cached=True' option\n")
    start_datetime = datetime.datetime.strptime('01 Aug 2018', '%d %b %Y').replace(tzinfo=pytz.utc)
    """ we need a gap for testing """
    end_datetime = floor_time(datetime.datetime.now(timezone.utc) - datetime.timedelta(minutes=1))
    print(f'Start datetime - end datetime: {start_datetime} - {end_datetime}\n')
    _data_df = fetcher.resample_to_timeframe(table_name="spot_data_btcusdt_1m",
                                             start=start_datetime,
                                             end=end_datetime,
                                             to_timeframe="1h",
                                             origin="start",
                                             use_cols=Constants.ohlcv_cols,
                                             use_dtypes=Constants.ohlcv_dtypes,
                                             open_time_index=False,
                                             cached=True)
    print(_data_df.head(2).to_string())
    print(_data_df.tail(1).to_string())
    _data_df_1 = fetcher.resample_to_timeframe(table_name="spot_data_btcusdt_1m",
                                               start=start_datetime,
                                               end=end_datetime,
                                               to_timeframe="1h",
                                               origin="start",
                                               use_cols=Constants.ohlcv_cols,
                                               use_dtypes=Constants.ohlcv_dtypes,
                                               open_time_index=False,
                                               cached=True)
    print(_data_df_1.head(2).to_string())
    print(_data_df_1.tail(1).to_string())
    print("\nCompare result:\n")
    print(_data_df.compare(_data_df_1, align_axis=0).to_string())
    fetcher2 = DataFetcher(host=ConfigPostgreSQL.HOST,
                           database=ConfigPostgreSQL.DATABASE,
                           user=ConfigPostgreSQL.USER,
                           password=ConfigPostgreSQL.PASSWORD,
                           binance_api_key='dummy',
                           binance_api_secret='dummy',
                           )
    print(f'Start datetime - end datetime: {start_datetime} - {end_datetime}\n')
    print("\nResampling of same period and compare results with 'cached=False' option\n")
    print(f'Start datetime - end datetime: {start_datetime} - {end_datetime}')
    _data_df_3 = fetcher2.resample_to_timeframe(table_name="spot_data_btcusdt_1m",
                                                start=start_datetime,
                                                end=end_datetime,
                                                to_timeframe="1h",
                                                origin="start",
                                                use_cols=Constants.ohlcv_cols,
                                                use_dtypes=Constants.ohlcv_dtypes,
                                                open_time_index=False,
                                                cached=False)
    print(_data_df_3.head(2).to_string())
    print(_data_df_3.tail(1).to_string())
    print("\nCompare result:\n")
    print(_data_df.compare(_data_df_3, align_axis=0).to_string())
    """ End testing resample_to_timeframe method """
