import os
import pytz

import asyncpg
import asyncio
import logging
import datetime
import numpy as np
import pandas as pd

from typing import Tuple, Union, Optional, List, Any
from datetime import timezone

from pandas import Timestamp

from dbbinance.fetcher.constants import Constants
from dbbinance.fetcher import AsyncSQLMeta
from dbbinance.fetcher import AsyncPool
from dbbinance.fetcher import create_pool
from dbbinance.fetcher import ceil_time, floor_time

from dbbinance.fetcher.fetchercachemanager import FetcherCacheManager
from dbbinance.fetcher.datautils import convert_timeframe_to_freq
from collections import OrderedDict

from binance.client import Client
from binance.exceptions import BinanceAPIException

# from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dbbinance.config.configpostgresql import ConfigPostgreSQL
from dbbinance.config.configbinance import ConfigBinance

__version__ = 0.79

logger = logging.getLogger()


class AsyncPostgreSQLDatabase(AsyncSQLMeta):
    def __init__(self, pool):
        super().__init__(pool)
        self.max_verbose = False

    @staticmethod
    def prepare_table_name(market: str = None, symbol_pair: str = "BTCUSDT") -> str:
        if market is None:
            market = "spot"
        base_table_name = f"{market}_data"
        timeframe = "1m"
        table_name = f"{base_table_name}_{symbol_pair}_{timeframe}".lower()
        return table_name

    async def is_data_exists(self, table_name: str) -> bool:
        query = f"SELECT 1 FROM {table_name} LIMIT 1"
        result = await self.asyncdb_mgr.single_select_query(query)
        return result is not None

    async def create_table(self, table_name: str):
        """
        Creates the optimized table with TIMESTAMPTZ + FLOAT8
        """
        columns_definition = [
            "open_time TIMESTAMPTZ NOT NULL PRIMARY KEY",
            "open FLOAT8",
            "high FLOAT8",
            "low FLOAT8",
            "close FLOAT8",
            "volume FLOAT8",
            "close_time TIMESTAMPTZ",
            "quote_asset_volume FLOAT8",
            "trades INTEGER",
            "taker_buy_base FLOAT8",
            "taker_buy_quote FLOAT8",
            "ignored FLOAT8"
        ]

        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns_definition)});"
        await self.asyncdb_mgr.modify_query(query)

        # Recommended indexes
        await self.asyncdb_mgr.modify_query(
            f"CREATE UNIQUE INDEX IF NOT EXISTS {table_name}_open_time_idx ON {table_name} (open_time);")
        await self.asyncdb_mgr.modify_query(
            f"CREATE INDEX IF NOT EXISTS {table_name}_open_time_brin_idx ON {table_name} USING BRIN (open_time);")

        logger.debug(f"{self.__class__.__name__}: Created table '{table_name}' with indexes")

    async def insert_kline_to_table(self, table_name: str, kline: list) -> int:
        """
        Bulk insert klines using UNNEST + numpy slicing.

        Returns:
            number of inserted rows
        """
        return await self.insert_klines_to_table(table_name, kline)

    async def insert_klines_to_table_fast(self, table_name: str, klines: list) -> int:
        """
        Fast bulk insert K-lines using UNNEST + NumPy slicing.
        Returns number of rows actually inserted.
        """

        if not klines:
            return 0

        try:
            arr = np.asarray(klines, dtype=object)

            # Process timestamps (ms -> datetime UTC) in one pass
            open_time = []
            close_time = []
            for ts_open, ts_close in arr[:, [0, 6]]:
                open_time.append(datetime.datetime.fromtimestamp(int(ts_open) / 1000, tz=timezone.utc))
                close_time.append(datetime.datetime.fromtimestamp(int(ts_close) / 1000, tz=timezone.utc))

            # Numeric/int columns: convert to appropriate dtype but keep as NumPy arrays
            open_ = arr[:, 1].astype(np.float64)
            high = arr[:, 2].astype(np.float64)
            low = arr[:, 3].astype(np.float64)
            close = arr[:, 4].astype(np.float64)
            volume = arr[:, 5].astype(np.float64)

            quote_asset_volume = arr[:, 7].astype(np.float64)
            trades = arr[:, 8].astype(np.int32)
            taker_buy_base = arr[:, 9].astype(np.float64)
            taker_buy_quote = arr[:, 10].astype(np.float64)
            ignored = arr[:, 11].astype(np.float64)

            query = f"""
            WITH inserted AS (
                INSERT INTO {table_name} (
                    open_time, open, high, low, close, volume, close_time,
                    quote_asset_volume, trades, taker_buy_base, taker_buy_quote, ignored
                )
                SELECT *
                FROM unnest(
                    $1::timestamptz[],
                    $2::float8[], $3::float8[], $4::float8[], $5::float8[], $6::float8[],
                    $7::timestamptz[],
                    $8::float8[], $9::int[], $10::float8[], $11::float8[], $12::float8[]
                )
                ON CONFLICT (open_time) DO NOTHING
                RETURNING 1
            )
            SELECT COUNT(*) FROM inserted;
            """

            async with AsyncPool(self.pool) as conn:
                inserted_count = await conn.fetchval(
                    query,
                    open_time,
                    open_,
                    high,
                    low,
                    close,
                    volume,
                    close_time,
                    quote_asset_volume,
                    trades,
                    taker_buy_base,
                    taker_buy_quote,
                    ignored
                )

            inserted_count = int(inserted_count or 0)
            logger.info(f"{self.__class__.__name__}: Inserted {inserted_count}/{len(klines)} K-lines into {table_name}")
            return inserted_count

        except Exception:
            logger.exception(f"{self.__class__.__name__}: Error inserting K-lines into '{table_name}'")
            raise

    def logger_debug(self, msg):
        if self.max_verbose:
            logger.debug(msg)

    async def get_min_open_time(self, table_name: str, retry=7) -> int:
        try:
            query = f"SELECT MIN(open_time) FROM {table_name}"
            for _ in range(retry + 1):
                async with AsyncPool(self.pool) as conn:
                    min_open_time = await conn.fetchval(query)
                    if min_open_time is not None:
                        return int(min_open_time.timestamp() * 1000)
                await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"{self.__class__.__name__}: {e}")
        # default
        dt = datetime.datetime.strptime("01 Jan 2017", "%d %b %Y")
        return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)

    async def get_max_open_time(self, table_name: str, retry=7) -> int:
        try:
            query = f"SELECT MAX(open_time) FROM {table_name}"
            for _ in range(retry + 1):
                async with AsyncPool(self.pool) as conn:
                    max_open_time = await conn.fetchval(query)
                    if max_open_time is not None:
                        return int(max_open_time.timestamp() * 1000)
                await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"{self.__class__.__name__}: {e}")
        # fallback
        return int(datetime.datetime.now(timezone.utc).timestamp() * 1000)


class DataRepair:
    def __init__(self, client_cls: Client):
        self.client = client_cls
        self.data_df = pd.DataFrame()

        logger.debug(f"{self.__class__.__name__}: Initializing {self.__class__.__name__}")

        self.open_time_col_name = "open_time"
        self.start_id: int = 1
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
        """ Remember 1st ID to reconstruct at the end """
        if "id" in data_df.columns:
            self.start_id = data_df["id"][0]

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
        """ Reset index to get 'open_time' as column"""
        if 'id' in data_df.columns:
            data_df = data_df.reset_index()
            """ Reconstruct 'id' column """
            data_df = data_df.drop(columns=["id"])
            data_df.insert(0, "id", range(self.start_id, self.start_id + len(data_df)))
            data_df["id"] = data_df["id"].astype("int64")

            """ Convert _datetime_ of 'open_time' to timestamp format """
            data_df["open_time"] = self.convert_datetime_to_timestamp(data_df["open_time"])
            self.ts_start_end_open_time_after = (data_df["open_time"].iloc[0], data_df["open_time"].iloc[-1])
            logger.debug(f"{self.__class__.__name__}: Start/end timestamp of 'open_time' before: "
                         f"{self.ts_start_end_open_time_before}")
            logger.debug(f"{self.__class__.__name__}: Start/end timestamp of 'open_time' after: "
                         f"{self.ts_start_end_open_time_after}")

            """ Create new close_time and ad it to data_df """
            frequency = convert_timeframe_to_freq(timeframe)
            close_time_dt = pd.date_range(self.start_end_close_time[0], self.start_end_close_time[1], freq=frequency)

            """ Convert _datetime_ of 'close_time' to timestamp format and insert back """
            data_df.insert(7, "close_time", self.convert_datetime_to_timestamp(pd.Series(close_time_dt)))
        else:
            """ Convert _datetime_ of 'open_time' to timestamp format """
            data_df["open_time"] = self.convert_datetime_to_timestamp(data_df["open_time"])
            self.ts_start_end_open_time_after = (data_df["open_time"].iloc[0], data_df["open_time"].iloc[-1])
            logger.debug(f"{self.__class__.__name__}: Start/end timestamp of 'open_time' before: "
                         f"{self.ts_start_end_open_time_before}")
            logger.debug(f"{self.__class__.__name__}: Start/end timestamp of 'open_time' after: "
                         f"{self.ts_start_end_open_time_after}")

            """ Create new close_time and ad it to data_df """
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
                logger.info(
                    f"{self.__class__.__name__}: Total missed data windows/timeframes: {len(self.timeframes_windows)}/{total_timeframes}")
                for ix, window_data in enumerate(self.timeframes_windows):
                    logger.debug(f"Window #{ix}: {window_data}")
                    nan_window_start_end[0], nan_window_start_end[1], nan_window_len = window_data

                    kdata = self.get_kline_period(nan_window_start_end[0], nan_window_start_end[1], symbol_pair,
                                                  timeframe)
                    if kdata:
                        temp_df = pd.DataFrame(kdata,
                                               columns=list(Constants.binance_cols),
                                               )
                        temp_df['open_time'] = pd.to_datetime(temp_df['open_time'],
                                                              unit='ns',
                                                              infer_datetime_format=True,
                                                              utc=True,
                                                              )
                        # temp_df[self.open_time_col_name] = pd.to_datetime(temp_df[self.open_time_col_name],
                        #                                                   unit='ms')
                        temp_df[self.open_time_col_name] = pd.to_datetime(temp_df[self.open_time_col_name],
                                                                          # unit='ms',
                                                                          infer_datetime_format=True,
                                                                          format=Constants.default_datetime_format)

                        temp_df[self.open_time_col_name] = self.datetime_round(temp_df[self.open_time_col_name],
                                                                               frequency)

                        temp_df = temp_df.drop(columns=["close_time"])
                        """ 
                        Add correct window START and END timeframes to calculate correct offsets
                        """
                        start_flag = False
                        end_flag = False

                        try:
                            pd_infer_freq = pd.infer_freq(temp_df[self.open_time_col_name])
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
                            pd_infer_freq = pd.infer_freq(temp_df[self.open_time_col_name])
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
                            double_checked_df = self.repair_index(to_add_set, to_delete_set, temp_df)
                        else:
                            double_checked_df = temp_df

                        data_df_repair_window_start_row = data_df.index.get_loc(double_checked_df.index[0])
                        data_df_repair_window_end_row = data_df.index.get_loc(double_checked_df.index[-1])

                        data_df.iloc[data_df_repair_window_start_row:data_df_repair_window_end_row + 1,
                        :] = double_checked_df

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
                    f"{self.__class__.__name__}: Total/loaded/NOT loaded timeframes: {total_timeframes}/{loaded_timeframes}/{not_loaded_timeframes}")

                """ Drop id and reconstruct """
                data_df = self.after_preparation(data_df, timeframe)

                for col_name in data_df.columns:
                    dict_name = col_name
                    if col_name[0].isupper():
                        dict_name = dict_name[0].lower()
                    col_dtype = Constants.newsql_dtypes.get(dict_name, None)
                    if col_name == "id":
                        col_dtype = "int64"
                    data_df[col_name] = data_df[col_name].astype(col_dtype)
        else:
            logger.debug(f"{self.__class__.__name__}: Data is ok. Repairing skipped...")
        return data_df


updater_status: bool = False


class AsyncDataUpdaterMeta(AsyncPostgreSQLDatabase):
    updater_is_running = updater_status

    def __init__(self, pool: Optional[asyncpg.Pool], binance_api_key, binance_api_secret,
                 symbol_pairs=None, timeframes=None):
        super().__init__(pool)
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
                                 'number_of_trades': 'sum', 'taker_buy_base_asset_volume': 'sum',
                                 'taker_buy_quote_asset_volume': 'sum'}

        self.async_scheduler = None
        self.last_timeframe_datetime = None

    async def prepare_start_end(self,
                                table_name: str,
                                start: Optional[Union[datetime.datetime, int, str]],
                                end: Optional[Union[datetime.datetime, int, str]]
                                ) -> Tuple[datetime.datetime, datetime.datetime]:
        """
        Normalize start/end to UTC datetime objects for querying new TIMESTAMPTZ table.

        Returns:
            Tuple[start_datetime_utc, end_datetime_utc]
        """

        # -----------------------------
        # Convert start to datetime UTC
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
            start_dt = datetime.datetime.fromtimestamp(await self.get_min_open_time(table_name) / 1000, tz=timezone.utc)

        # -----------------------------
        # Convert end to datetime UTC
        # -----------------------------
        if end is None:
            end_dt = datetime.datetime.fromtimestamp(await self.get_max_open_time(table_name) / 1000, tz=timezone.utc)
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
        min_open_time = datetime.datetime.fromtimestamp(await self.get_min_open_time(table_name) / 1000,
                                                        tz=timezone.utc)
        if start_dt < min_open_time:
            start_dt = min_open_time

        return start_dt, end_dt


class AsyncDataUpdater(AsyncDataUpdaterMeta):
    updater_is_running = updater_status

    def __init__(self, pool, binance_api_key, binance_api_secret,
                 symbol_pairs=None, timeframes=None):
        super().__init__(pool, binance_api_key, binance_api_secret, symbol_pairs, timeframes)
        self.loop = asyncio.get_running_loop()
        # 1. Create a Future tied to the current running loop
        self.notification_future = self.loop.create_future()

    async def check_first_run(self):
        """ Check if this a first run """
        logger.info(f"{self.__class__.__name__} #{self.idnum}: Checking the 1st run")
        for base_table_name in self.base_tables_names:
            for symbol_pair in self.symbol_pairs:
                for timeframe in self.timeframes:
                    table_name = f"{base_table_name}_{symbol_pair}_{timeframe}".lower()
                    # 1. Check if table exists
                    if not await self.is_table_exists(table_name):
                        logger.info(f"{self.__class__.__name__} #{self.idnum}: Creating table '{table_name}'")
                        await self.create_table(table_name)
                        # await self.is_duplicates_exists(table_name)

                        # 2. WAIT for data fetching to finish before next line
                        # Since fetch_historical_spot_data is async def, await it directly.
                        await self.fetch_historical_spot_data(base_table_name, symbol_pair, timeframe)

                        await self.check_and_repair(table_name, symbol_pair, timeframe)
                        continue

                    # 3. Check if data exists
                    if not await self.is_data_exists(table_name):
                        logger.info(f"{self.__class__.__name__} #{self.idnum}: Data empty. Fetching...")

                        # WAIT for results here
                        await self.fetch_historical_spot_data(base_table_name, symbol_pair, timeframe)

                        await self.check_and_repair(table_name, symbol_pair, timeframe)
                    else:
                        logger.debug(f"{self.__class__.__name__} #{self.idnum}: Updating {symbol_pair}")
                        await self.check_and_repair(table_name, symbol_pair, timeframe)
                        await self.update_spot_data()

    async def fetch_historical_spot_data(self, base_table_name, symbol_pair, timeframe):
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

        result = await self.insert_klines_to_table(table_name, klines)

        logger.debug(f"{self.__class__.__name__} #{self.idnum}: Finished saving historical data to database for "
                     f"{symbol_pair}, {timeframe} at: {datetime.datetime.now(timezone.utc)}. Written qty: {result} "
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

    async def update_spot_data(self):
        logger.debug(f"{self.__class__.__name__} #{self.idnum}: Update started...")
        # self.cur = self.conn.cursor()
        for base_table_name in self.base_tables_names:
            for symbol_pair in self.symbol_pairs:
                for timeframe in self.timeframes:
                    table_name = f"{base_table_name}_{symbol_pair}_{timeframe}".lower()
                    start_open_time = await self.get_max_open_time(table_name)
                    start_open_time = datetime.datetime.fromtimestamp(start_open_time / 1000, tz=pytz.utc)
                    start_open_time = start_open_time + datetime.timedelta(
                        minutes=self.convert_timeframe_to_min(timeframe))
                    until_open_time = start_open_time + datetime.timedelta(days=365)

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
                        if klines:
                            self.last_timeframe_datetime = datetime.datetime.fromtimestamp(klines[-1][0] / 1000,
                                                                                           tz=pytz.utc)
                            if len(klines) == 1:
                                result = await self.insert_kline_to_table(table_name, klines[0])
                            else:
                                result = await self.insert_klines_to_table(table_name, klines)

                            logger.info(
                                f"{self.__class__.__name__} #{self.idnum}: Updater - {table_name} - "
                                f"Written timeframes: {result}/{len(klines)}. "
                                f"Last timeframe: {self.last_timeframe_datetime}")
                        logger.debug(f"{self.__class__.__name__} #{self.idnum}: Updating '{table_name}' finished...")
                    # for kline in klines:
                    #     self.insert_kline_to_table(table_name, kline)
        logger.debug(f"{self.__class__.__name__} #{self.idnum}: Update of all symbols finished...")

    async def get_data_as_df(self,
                             table_name: str,
                             start: Union[datetime.datetime, int],
                             end: Union[datetime.datetime, int],
                             use_cols=Constants.newsql_cols,
                             use_dtypes=None,
                             ) -> pd.DataFrame:

        _start_time = datetime.datetime.now(timezone.utc)
        if use_dtypes is None:
            use_dtypes = Constants.newsql_dtypes

        start_timestamp, end_timestamp = await self.prepare_start_end(table_name, start, end)

        # Join column names without sql.Identifier and format them into the query string
        cols_str = ', '.join(['%s'] * len(use_cols)) % tuple(use_cols)
        query = f"""
            SELECT {cols_str} 
            FROM {table_name} 
            WHERE open_time >= $1
            AND open_time <= $2
            ORDER BY open_time ASC
         """

        logger.debug(f"{self.__class__.__name__} #{self.idnum}: "
                     f"Start getting historical data from database table {table_name} at: {_start_time}")

        data = await self.asyncdb_mgr.select_query(query,
                                                   (start_timestamp, end_timestamp))  # Use parameters in the query

        logger.debug(f"{self.__class__.__name__} #{self.idnum}: "
                     f"Finished getting historical data from database table {table_name} at: "
                     f"{datetime.datetime.now(timezone.utc)}. "
                     f"ETA: {datetime.datetime.now(timezone.utc) - _start_time}")

        data_df = pd.DataFrame(data, columns=list(use_cols))
        data_df = data_df.astype(use_dtypes)
        return data_df

    async def is_symbol_pair_available(self, market, symbol_pair) -> bool:
        base_table_name = f"{market}_data"
        timeframe = "1m"
        table_name = f"{base_table_name}_{symbol_pair}_{timeframe}".lower()
        status = await self.is_table_exists(table_name)
        return status

    async def get_all_data_as_df(self, table_name, use_cols=Constants.newsql_cols, use_dtypes=None) -> pd.DataFrame:
        if use_dtypes is None:
            use_dtypes = Constants.newsql_dtypes

        start_open_time = await self.get_min_open_time(table_name)
        end_open_time = await self.get_max_open_time(table_name)
        data_df = await self.get_data_as_df(table_name, start_open_time, end_open_time, use_cols, use_dtypes)
        return data_df

    async def check_and_repair(self, table_name, symbol_pair, timeframe):
        start_time = datetime.datetime.now(timezone.utc)
        logger.debug(f"{self.__class__.__name__} #{self.idnum}: "
                     f"Check & repair {table_name}. "
                     f"Loading data. ETA: {datetime.datetime.now(timezone.utc) - start_time}")
        data_df = await self.get_all_data_as_df(table_name)

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
            data_df = data_df.drop(columns="id")
            result = await self.insert_klines_to_table(table_name, data_df.as_list())
            logger.debug(f"{self.__class__.__name__} #{self.idnum}:'{table_name}' - data saved. "
                         f"Written qty: {result}. ETA: {datetime.datetime.now(timezone.utc) - start_time}")
        else:
            logger.info(f"{self.__class__.__name__} #{self.idnum}: "
                        f"'{table_name}' - Database already repaired. Don't need any action.")

    async def start_background_updater(self,
                                       sleep_time: int = 60,  # in seconds
                                       ):
        # Create a new async scheduler
        update_interval = int(sleep_time // 60)  # in minutes

        if not AsyncDataUpdater.updater_is_running:
            self.async_scheduler = AsyncIOScheduler()

            print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

            """ if updater is not running, check and repair database before we run updater """
            await self.check_first_run()

            back_start_time = ceil_time(datetime.datetime.now(), "1m").replace(second=2, microsecond=0)
            print('Updater will start at', back_start_time)
            # Add a new job to the scheduler to run the update_database method every minute
            self.async_scheduler.add_job(self.update_spot_data,
                                         'interval',
                                         minutes=update_interval,
                                         next_run_time=back_start_time,
                                         )
            try:
                # Start the scheduler
                self.async_scheduler.start()
                AsyncDataUpdater.updater_is_running = True
                while True:
                    try:
                        await asyncio.sleep(sleep_time)
                    except (KeyboardInterrupt, SystemExit) as e:
                        raise e
            except (KeyboardInterrupt, SystemExit):
                print('Stopping updater...')
                self.async_scheduler.shutdown()  # Shut down the scheduler on interruption
                AsyncDataUpdater.updater_is_running = False
            except Exception as e:
                print(f'Unexpected error occurred: {e}')
                self.async_scheduler.shutdown()
                AsyncDataUpdater.updater_is_running = False
                raise


cache_manager_obj = FetcherCacheManager(max_memory_gb=2)


class AsyncDataFetcher(AsyncDataUpdaterMeta):
    CM = cache_manager_obj

    def __init__(self, pool, binance_api_key, binance_api_secret,
                 cache_obj: Optional[FetcherCacheManager] = None):
        if cache_obj is not None:
            self.set_new_cache_obj(cache_obj)
        super().__init__(pool, binance_api_key, binance_api_secret)

    @classmethod
    def set_new_cache_obj(cls, cache_obj):
        cls.CM = cache_obj

    def _get_agg_dict(self, use_cols):
        actual_agg_dict = OrderedDict()
        for col_name in use_cols:
            if col_name != "open_time":
                actual_agg_dict.update({col_name: self.default_agg_dict.get(col_name, None)})
        return actual_agg_dict

    async def resample_to_timeframe(self,
                                    table_name: str,
                                    start: Union[datetime.datetime, int],
                                    end: Union[datetime.datetime, int],
                                    to_timeframe: str = "1h",
                                    origin: str = "start",
                                    use_cols: tuple = Constants.binance_cols,
                                    use_dtypes=None,
                                    open_time_index: bool = True,
                                    cached=False,
                                    ):
        # prepare the start and end timestamps, convert them to datetime if necessary
        start_timestamp, end_timestamp = await self.prepare_start_end(table_name, start, end)

        async def fetch_raw_data():
            query = f"""
                SELECT {', '.join(use_cols)}
                FROM {table_name}
                WHERE open_time >= $1
                  AND open_time <= $2
                ORDER BY open_time ASC
            """

            async with AsyncPool(self.pool) as conn:
                return await conn.fetch(query, start_timestamp, end_timestamp)

        async def prepare_raw_df():
            _df = await fetch_raw_data()
            _df = pd.DataFrame(_df, columns=list(use_cols))
            _df = _df.astype(use_dtypes)
            _df = _df.sort_values(by=['open_time']).set_index('open_time', inplace=False, drop=False)
            return _df

        async def get_raw_df():
            # check if the data is already cached
            cache_key = self.CM.get_cache_key(table_name=table_name, start_timestamp=start_timestamp,
                                              end_timestamp=end_timestamp)

            if cached and (cache_key in self.CM.cache):
                raw_df = self.CM.cache[cache_key]
                print(f"{self.__class__.__name__} #{self.idnum}: Return cached RAW data: {table_name} / "
                      f"{start_timestamp} - {end_timestamp}")
            else:
                # if not cached, fetch the data and cache it
                raw_df = await prepare_raw_df()
                self.CM.update_cache(key=cache_key, value=raw_df.copy(deep=True))

            return raw_df.copy(deep=True)

        async def prepare_resampled_df():
            resampled_df = await get_raw_df()
            # convert timestamps to datetime objects
            resampled_df['open_time'] = pd.to_datetime(resampled_df['open_time'], unit='ns', infer_datetime_format=True,
                                                       utc=True, )
            resampled_df = resampled_df.set_index('open_time', inplace=False)

            # prepare aggregation dictionary for the 'agg' method of pandas DataFrame
            agg_dict = self._get_agg_dict(use_cols)
            freq = convert_timeframe_to_freq(to_timeframe)

            resampled_df = resampled_df.resample(freq, label='right', closed='right', origin=origin).agg(agg_dict)
            if not open_time_index:
                resampled_df = resampled_df.reset_index()

            print(f"{self.__class__.__name__} #{self.idnum}: "
                  f"'{table_name}' resample to timeframe: '{freq}', "
                  f"from origin: {origin}, start-end: {start_timestamp} - {end_timestamp}")

            return resampled_df

        async def get_resampled_df():
            if cached:
                cache_key = self.CM.get_cache_key(table_name=table_name, start_timestamp=start_timestamp,
                                                  end_timestamp=end_timestamp, timeframe=to_timeframe, origin=origin,
                                                  open_time_index=open_time_index)

                # check if the data is already cached
                if cache_key in self.CM.cache:
                    resampled_df = self.CM.cache[cache_key]
                    print(f"{self.__class__.__name__} #{self.idnum}: Return cached RESAMPLED data: {table_name} / "
                          f"{start_timestamp} - {end_timestamp}")
                else:
                    resampled_df = await prepare_resampled_df()
                    self.CM.update_cache(key=cache_key, value=resampled_df.copy(deep=True))
            else:
                resampled_df = await prepare_resampled_df()

            return resampled_df

        data_df = await get_resampled_df()

        return data_df.copy(deep=True)


async def main_tests():
    pool = await create_pool(host=ConfigPostgreSQL.HOST,
                             database=ConfigPostgreSQL.DATABASE,
                             user=ConfigPostgreSQL.USER,
                             password=ConfigPostgreSQL.PASSWORD)

    updater = AsyncDataUpdater(pool=pool,
                               binance_api_key=ConfigBinance.BINANCE_API_KEY,
                               binance_api_secret=ConfigBinance.BINANCE_API_SECRET,
                               )

    # print("Drop duplicates. Success: ", await updater.drop_duplicates_rows(table_name="spot_data_btcusdt_1m"))

    """ Start testing get_data_as_df method """
    start_datetime = datetime.datetime.strptime('01 Aug 2017', '%d %b %Y').replace(tzinfo=timezone.utc)
    _data_df = await updater.get_data_as_df(table_name="spot_data_btcusdt_1m",
                                            start=start_datetime,
                                            end=datetime.datetime.now(tz=timezone.utc),
                                            use_cols=Constants.ohlcv_cols,
                                            use_dtypes=Constants.ohlcv_dtypes
                                            )
    # _data_df['open_time'] = pd.to_datetime(_data_df['open_time'], unit='ms')
    # _data_df['open_time'] = pd.to_datetime(_data_df['open_time'],
    #                                        infer_datetime_format=True,
    #                                        format=Constants.default_datetime_format)
    #
    # _data_df['open_time'] = DataRepair.convert_timestamp_to_datetime(_data_df['open_time'])
    _data_df['open_time'] = pd.to_datetime(_data_df['open_time'],
                                           unit='ns',
                                           infer_datetime_format=True,
                                           utc=True,
                                           )

    _data_df = _data_df.set_index('open_time', inplace=False)

    print(_data_df.head(15).to_string())
    print(_data_df.tail(15).to_string())
    """ End testing get_data_as_df method """

    """ Start testing resample_to_timeframe method and 'cached' option"""
    fetcher = AsyncDataFetcher(pool=pool,
                               binance_api_key='dummy',
                               binance_api_secret='dummy',
                               )

    print("\nResampling of same period and compare results with 'cached=True' option\n")
    start_datetime = datetime.datetime.strptime('01 Aug 2018', '%d %b %Y').replace(tzinfo=timezone.utc)
    """ we need a gap for testing """
    end_datetime = floor_time(datetime.datetime.now(timezone.utc) - datetime.timedelta(minutes=1))
    print(f'Start datetime - end datetime: {start_datetime} - {end_datetime}\n')
    _data_df = await fetcher.resample_to_timeframe(table_name="spot_data_btcusdt_1m",
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
    _data_df_1 = await fetcher.resample_to_timeframe(table_name="spot_data_btcusdt_1m",
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
    fetcher2 = AsyncDataFetcher(pool=pool,
                                binance_api_key='dummy',
                                binance_api_secret='dummy',
                                )
    print(f'Start datetime - end datetime: {start_datetime} - {end_datetime}\n')
    print("\nResampling of same period and compare results with 'cached=False' option\n")
    print(f'Start datetime - end datetime: {start_datetime} - {end_datetime}')
    _data_df_3 = await fetcher2.resample_to_timeframe(table_name="spot_data_btcusdt_1m",
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


if __name__ == "__main__":
    logger.setLevel(logging.DEBUG)

    file_handler = logging.FileHandler('asyncdatafetcher.log')
    file_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    logging.getLogger('apscheduler').setLevel(logging.DEBUG)
    asyncio.run(main_tests())
