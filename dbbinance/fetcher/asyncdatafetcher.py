import os
import time

import asyncpg
import pytz
import asyncio
import logging
import datetime
import pandas as pd
from typing import Tuple, Union, Optional, List, Any
from datetime import timezone

from pandas import Timestamp

from dbbinance.fetcher.constants import Constants
from dbbinance.fetcher import AsyncSQLMeta
from dbbinance.fetcher import AsyncPool
from dbbinance.fetcher import create_pool

from dbbinance.fetcher.fetchercachemanager import FetcherCacheManager
from dbbinance.fetcher.datautils import convert_timeframe_to_freq
from collections import OrderedDict

from binance.client import Client
from binance.exceptions import BinanceAPIException

from apscheduler.schedulers.background import BackgroundScheduler
from dbbinance.config.configpostgresql import ConfigPostgreSQL
from dbbinance.config.configbinance import ConfigBinance

__version__ = 0.74

logger = logging.getLogger()


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


class AsyncPostgreSQLDatabase(AsyncSQLMeta):
    def __init__(self, pool):
        super().__init__(pool)
        self.max_verbose = False

    @staticmethod
    def prepare_table_name(market: str = None,
                           symbol_pair: str = "BTCUSDT") -> str:
        if market is None:
            market = "spot"
        base_table_name = f"{market}_data"
        timeframe = "1m"
        table_name = f"{base_table_name}_{symbol_pair}_{timeframe}".lower()
        return table_name

    async def is_data_exists(self, table_name):
        query = """
        SELECT * FROM {} LIMIT 1
        """
        _data = await self.asyncdb_mgr.single_select_query(
            query.format(table_name)
        )
        return _data is not None

    async def create_table(self, table_name: str):
        """
        Creates a new table in the database.

        Args:
            table_name  (str): Name of the table to create.

        Returns:
            None
        """
        columns_definition = [
            ("id", "SERIAL PRIMARY KEY"),
            ("open_time", "BIGINT"),
            ("open", "NUMERIC"),
            ("high", "NUMERIC"),
            ("low", "NUMERIC"),
            ("close", "NUMERIC"),
            ("volume", "NUMERIC"),
            ("close_time", "BIGINT"),
            ("quote_asset_volume", "NUMERIC"),
            ("trades", "INTEGER"),
            ("taker_buy_base", "NUMERIC"),
            ("taker_buy_quote", "NUMERIC"),
            ("ignored", "NUMERIC")
        ]

        # Build the CREATE TABLE query dynamically
        query = "CREATE TABLE {} ({})".format(
            table_name,
            ", ".join("{} {}".format(col, data_type) for col, data_type in columns_definition)
        )

        # Execute the constructed query asynchronously
        async with AsyncPool(self.pool) as conn:
            await conn.execute(query)

        logger.debug(f"{self.__class__.__name__}: Created table '{table_name}'")

    async def insert_kline_to_table(self, table_name: str, kline: list):
        """
        Insert a K-line record into the specified table.

        Args:
            table_name  (str): Name of the target table.
            kline       (list): Array of values representing the K-line data.

        Returns:
            None
        """
        # Unpack kline values
        open_time, open_price, high_price, low_price, close_price, volume, close_time, \
            quote_asset_volume, trades, taker_buy_base, taker_buy_quote, ignored = kline[:12]

        # Correct way to reference table name using Identifier
        query = """
            INSERT INTO {table} (
                open_time, open, high, low, close, volume, close_time, 
                quote_asset_volume, trades, taker_buy_base, taker_buy_quote, ignored
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12);
         """.format(table=table_name)

        # Execute the query with tuple of values
        async with AsyncPool(self.pool) as conn:
            await conn.execute(query, open_time, open_price, high_price, low_price, close_price, volume,
                               close_time, quote_asset_volume, trades, taker_buy_base, taker_buy_quote, ignored)

    async def insert_klines_to_table(self, table_name: str, klines: list):
        """
       Insert a K-line records into the specified table.

       Args:
           table_name   (str): Name of the target table.
           klines       (list): Array of values representing the K-line data.

       Returns:
           None
        """

        query = """
             INSERT INTO {table} (open_time, open, high, low, close, volume, close_time, 
             quote_asset_volume, trades, taker_buy_base, taker_buy_quote, ignored)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12);
          """.format(table=table_name)
        async with AsyncPool(self.pool) as conn:
            await conn.execute("BEGIN;")  # Begin transaction

            # Exclusive lock the table before inserting
            await conn.execute("LOCK TABLE {} IN ACCESS EXCLUSIVE MODE;".format(table_name))

            # Execute many query to insert multiple rows at once
            await conn.executemany(query, klines)

    def logger_debug(self, msg):
        if self.max_verbose:
            logger.debug(msg)

    async def get_min_open_time(self, table_name: str, retry=7) -> int:
        self.logger_debug(
            f"{self.__class__.__name__}: get_min_open_time called with table_name={table_name} and retry={retry}")

        try:
            query = """SELECT MIN(open_time) FROM {}""".format(table_name)
            self.logger_debug(f"{self.__class__.__name__}: Executing query: {query}")

            for _ in range(retry + 1):
                try:
                    async with AsyncPool(self.pool) as conn:  # Use a separate connection for each attempt.
                        min_open_time = await conn.fetchval(query)
                        if min_open_time is not None:
                            break
                    await asyncio.sleep(1)  # Wait before next retry.
                except Exception as e:
                    logger.error(f"{self.__class__.__name__}: An error occurred while executing the query: {e}")

            if min_open_time is not None:
                self.logger_debug(f"{self.__class__.__name__}: Query returned min_open_time={min_open_time}")
            else:
                self.logger_debug(f"{self.__class__.__name__}: No rows found or no more retries left")
        except Exception as e:
            logger.error(f"{self.__class__.__name__}: An error occurred while executing the query: {e}")
            min_open_time = None

        if (not min_open_time) or (min_open_time is None):
            min_open_time = datetime.datetime.strptime('01 Jan 2017', '%d %b %Y')
            min_open_time = int(datetime.datetime.timestamp(min_open_time) * 1000)
            self.logger_debug(f"{self.__class__.__name__}: Returning default value: {min_open_time}")
        else:
            self.logger_debug(f"{self.__class__.__name__}: Returning min_open_time: {min_open_time}")

        return min_open_time

    async def get_max_open_time(self, table_name: str, retry=7) -> int:
        self.logger_debug(
            f"{self.__class__.__name__}:get_max_open_time called with table_name={table_name} and retry={retry}")

        try:
            query = """SELECT MAX(open_time) FROM {}""".format(table_name)
            self.logger_debug(f"{self.__class__.__name__}: Executing query: {query}")

            for _ in range(retry + 1):
                async with AsyncPool(self.pool) as conn:  # Use a separate connection for each attempt.
                    try:
                        max_open_time = await conn.fetchval(query)
                        if max_open_time is not None:
                            break
                    except Exception as e:
                        logger.error(f"{self.__class__.__name__}: An error occurred while executing the query: {e}")
                await asyncio.sleep(1)  # Wait before next retry.

            if max_open_time is not None:
                self.logger_debug(f"{self.__class__.__name__}: Query returned max_open_time={max_open_time}")
            else:
                self.logger_debug(f"{self.__class__.__name__}: No rows found or no more retries left")
        except Exception as e:
            logger.error(f"{self.__class__.__name__}: An error occurred while executing the query: {e}")
            max_open_time = None

        if (not max_open_time) or (max_open_time is None):
            max_open_time = datetime.datetime.now(timezone.utc)
            max_open_time = int(datetime.datetime.timestamp(max_open_time) * 1000)
            self.logger_debug(f"{self.__class__.__name__}: Returning current timestamp: {max_open_time}")
        else:
            self.logger_debug(f"{self.__class__.__name__}: Returning max_open_time: {max_open_time}")

        return max_open_time

    async def is_duplicates_exists(self, table_name, show=True) -> bool:
        query = """
            SELECT open_time, COUNT(*)
            FROM {table}
            GROUP BY open_time
            HAVING COUNT(*) > 1;
        """.format(table=table_name)

        duplicates = await self.asyncdb_mgr.select_query(query)
        status = False
        if duplicates:
            status = True
            if show:
                logger.warning(f"{self.__class__.__name__}: Duplicates:")
                for row in duplicates:
                    logger.warning(f"{self.__class__.__name__}: {row}")
        return status

    async def drop_duplicates_rows(self, table_name):
        query = """
            DELETE FROM {table}
            WHERE id IN (
                SELECT id
                FROM (
                    SELECT id,
                           ROW_NUMBER() OVER(PARTITION BY open_time ORDER BY id) AS row_num
                    FROM {table}
                ) t
                WHERE t.row_num > 1
            );
        """.format(table=table_name)

        if not await self.asyncdb_mgr.modify_query(query):
            logger.warning(f"{self.__class__.__name__}: Can't drop duplicates:")


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
        data_df = data_df.reset_index()

        """ Reconstruct 'id' column """
        data_df = data_df.drop(columns=["id"])

        data_df.insert(0, "id", range(self.start_id, self.start_id + len(data_df)))
        data_df["id"] = data_df["id"].astype("int64")

        """ Convert _datetime_ of 'open_time' to timestamp format """
        data_df["open_time"] = self.convert_datetime_to_timestamp(data_df["open_time"])
        self.ts_start_end_open_time_after = (data_df["open_time"].iloc[0], data_df["open_time"].iloc[-1])
        logger.debug(
            f"{self.__class__.__name__}: Start/end timestamp of 'open_time' before: {self.ts_start_end_open_time_before}")
        logger.debug(
            f"{self.__class__.__name__}: Start/end timestamp of 'open_time' after: {self.ts_start_end_open_time_after}")

        """ Create new close_time and ad it to data_df """
        frequency = convert_timeframe_to_freq(timeframe)
        close_time_dt = pd.date_range(self.start_end_close_time[0], self.start_end_close_time[1], freq=frequency)

        """ Convert _datetime_ of 'close_time' to timestamp format and insert back """
        data_df.insert(7, "close_time", self.convert_datetime_to_timestamp(pd.Series(close_time_dt)))
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
                        temp_df[self.open_time_col_name] = pd.to_datetime(temp_df[self.open_time_col_name],
                                                                          unit='ms')
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
                    col_dtype = Constants.sql_dtypes.get(dict_name, None)
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

        self.back_scheduler = None
        self.last_timeframe_datetime = None

    async def prepare_start_end(self,
                                table_name: str,
                                start: Optional[Union[datetime.datetime, int, str]],
                                end: Optional[Union[datetime.datetime, int, str]]) -> Tuple[int, int]:

        if isinstance(start, int):
            start_timestamp = int(start)
        elif isinstance(start, str):
            start_timestamp = datetime.datetime.strptime(start,
                                                         Constants.default_datetime_format).replace(tzinfo=pytz.utc)
            start_timestamp = int(start_timestamp.timestamp() * 1000)
        else:
            start_timestamp = int(start.timestamp() * 1000)

        if end is None:
            end_timestamp = int(await self.get_max_open_time(table_name))
        elif isinstance(end, int):
            end_timestamp = int(end)
        elif isinstance(end, str):
            end_timestamp = datetime.datetime.strptime(end,
                                                       Constants.default_datetime_format).replace(tzinfo=pytz.utc)
            end_timestamp = int(end_timestamp.timestamp() * 1000)
        else:
            end_timestamp = int(end.timestamp() * 1000)

        start_open_time = int(await self.get_min_open_time(table_name))

        if start_timestamp < start_open_time or start_timestamp is None:
            start_timestamp = start_open_time

        return start_timestamp, end_timestamp


class AsyncDataUpdater(AsyncDataUpdaterMeta):
    updater_is_running = updater_status

    def __init__(self, pool, binance_api_key, binance_api_secret,
                 symbol_pairs=None, timeframes=None):
        super().__init__(pool, binance_api_key, binance_api_secret, symbol_pairs, timeframes)

    async def check_first_run(self):
        """ Check if this a first run """
        msg = f"{self.__class__.__name__} #{self.idnum}: Checking the 1st run"
        logger.info(msg)
        for base_table_name in self.base_tables_names:
            for symbol_pair in self.symbol_pairs:
                for timeframe in self.timeframes:
                    table_name = f"{base_table_name}_{symbol_pair}_{timeframe}".lower()
                    if not await self.is_table_exists(table_name):
                        msg = f"{self.__class__.__name__} #{self.idnum}: The table '{table_name}' is not exists. " \
                              f"Creating new table '{table_name}'"
                        logger.info(msg)
                        await self.create_table(table_name)
                        await self.is_duplicates_exists(table_name)
                        await self.fetch_historical_spot_data(base_table_name, symbol_pair, timeframe)
                        await self.check_and_repair(table_name, symbol_pair, timeframe)
                        continue
                    else:
                        if not await self.is_data_exists(table_name):
                            msg = f"{self.__class__.__name__} #{self.idnum}:The table '{table_name}' is present. " \
                                  f"But data is empty. Fetching data for symbol pair: {symbol_pair}"
                            logger.info(msg)
                            await self.fetch_historical_spot_data(base_table_name, symbol_pair, timeframe)
                            await self.check_and_repair(table_name, symbol_pair, timeframe)
                            continue
                        else:
                            msg = f"{self.__class__.__name__} #{self.idnum}: Updating database to " \
                                  f"actual {symbol_pair}, {timeframe}"
                            logger.debug(msg)
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

        await self.insert_klines_to_table(table_name, klines)

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
                            self.last_timeframe_datetime = datetime.datetime.fromtimestamp(
                                klines[-1][0] / 1000, tz=pytz.utc)
                            logger.info(
                                f"{self.__class__.__name__} #{self.idnum}: Updater - {table_name} - "
                                f"timeframes: {len(klines)}. Last timeframe: {self.last_timeframe_datetime}")
                            await self.insert_klines_to_table(table_name, klines)

                        logger.debug(f"{self.__class__.__name__} #{self.idnum}: Updating '{table_name}' finished...")
                    # for kline in klines:
                    #     self.insert_kline_to_table(table_name, kline)
        logger.debug(f"{self.__class__.__name__} #{self.idnum}: Update of all symbols finished...")

    async def get_data_as_df(self,
                             table_name: str,
                             start: Union[datetime.datetime, int],
                             end: Union[datetime.datetime, int],
                             use_cols=Constants.sql_cols,
                             use_dtypes=None,
                             ) -> pd.DataFrame:

        _start_time = datetime.datetime.now(timezone.utc)
        if use_dtypes is None:
            use_dtypes = Constants.sql_dtypes

        start_timestamp, end_timestamp = await self.prepare_start_end(table_name, start, end)

        # Join column names without sql.Identifier and format them into the query string
        cols_str = ', '.join(['%s'] * len(use_cols)) % tuple(use_cols)
        query = f"""
            SELECT {cols_str} 
            FROM {table_name} 
            WHERE open_time BETWEEN $1 AND $2
            ORDER BY open_time
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

    async def get_all_data_as_df(self, table_name, use_cols=Constants.sql_cols, use_dtypes=None) -> pd.DataFrame:
        if use_dtypes is None:
            use_dtypes = Constants.sql_dtypes

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
            await self.insert_klines_to_table(table_name, data_df.as_list())
            logger.debug(f"{self.__class__.__name__} #{self.idnum}: "
                         f"'{table_name}' - data saved. ETA: {datetime.datetime.now(timezone.utc) - start_time}")
        else:
            logger.info(f"{self.__class__.__name__} #{self.idnum}: "
                        f"'{table_name}' - Database already repaired. Don't need any action.")

    def start_background_updater(self,
                                 update_interval: int = 1,  # in minutes
                                 ):
        # Create a new background scheduler
        if not AsyncDataUpdater.updater_is_running:
            self.back_scheduler = BackgroundScheduler()

            """ if updater is not running, check and repair database before we run updater """
            self.check_first_run()

            back_start_time = ceil_time(datetime.datetime.now(), "1m")
            # Add a new job to the scheduler to run the update_database method every minute
            self.back_scheduler.add_job(self.update_spot_data,
                                        'interval',
                                        minutes=update_interval,
                                        start_date=back_start_time,
                                        )

            # Start the scheduler
            self.back_scheduler.start()
            AsyncDataUpdater.updater_is_running = True

    def test_background_updater(self, sleep_time: int = 60):

        self.start_background_updater(update_interval=int(sleep_time // 60))

        print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))
        try:
            # This is here to simulate application activity (which keeps the main thread alive).
            while True:
                time.sleep(sleep_time)
        except (KeyboardInterrupt, SystemExit):
            # Not strictly necessary if daemonic mode is enabled but should be done if possible
            AsyncDataUpdater.updater_is_running = False
            self.back_scheduler.shutdown()


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
            query = f"""SELECT {', '.join(use_cols)} 
                        FROM {table_name} 
                        WHERE open_time >= {start_timestamp} AND open_time <= {end_timestamp};
                    """

            # Use an async context manager to manage the connection
            async with AsyncPool(self.pool) as conn:
                raw_data = await conn.fetch(query)

            return raw_data

        async def prepare_raw_df():
            _df = pd.DataFrame(await fetch_raw_data(), columns=list(use_cols))
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
                      f"{start_timestamp}({DataRepair.convert_timestamp_to_datetime(start_timestamp)}) - "
                      f"{end_timestamp}({DataRepair.convert_timestamp_to_datetime(end_timestamp)}")
            else:
                # if not cached, fetch the data and cache it
                raw_df = await prepare_raw_df()
                self.CM.update_cache(key=cache_key, value=raw_df.copy(deep=True))

            return raw_df.copy(deep=True)

        async def prepare_resampled_df():
            resampled_df = await get_raw_df()

            # convert timestamps to datetime objects
            resampled_df['open_time'] = DataRepair.convert_timestamp_to_datetime(resampled_df['open_time'])
            resampled_df = resampled_df.set_index('open_time', inplace=False)

            # prepare aggregation dictionary for the 'agg' method of pandas DataFrame
            agg_dict = self._get_agg_dict(use_cols)
            freq = convert_timeframe_to_freq(to_timeframe)

            resampled_df = resampled_df.resample(freq, label='right', closed='right', origin=origin).agg(agg_dict)
            if not open_time_index:
                resampled_df = resampled_df.reset_index()

            print(f"{self.__class__.__name__} #{self.idnum}: "
                  f"'{table_name}' resample to timeframe: '{freq}', "
                  f"from origin: {origin}, start-end: "
                  f"{DataRepair.convert_timestamp_to_datetime(start_timestamp)} - "
                  f"{DataRepair.convert_timestamp_to_datetime(end_timestamp)}")

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
                          f"{start_timestamp}({DataRepair.convert_timestamp_to_datetime(start_timestamp)}) - "
                          f"{end_timestamp}({DataRepair.convert_timestamp_to_datetime(end_timestamp)}")
                else:
                    resampled_df = await prepare_resampled_df()
                    self.CM.update_cache(key=cache_key, value=resampled_df.copy(deep=True))
            else:
                resampled_df = await prepare_resampled_df()

            return resampled_df

        data_df = await get_resampled_df()

        return data_df.copy(deep=True)


async def main():
    pool = await create_pool(host=ConfigPostgreSQL.HOST,
                             database=ConfigPostgreSQL.DATABASE,
                             user=ConfigPostgreSQL.USER,
                             password=ConfigPostgreSQL.PASSWORD)

    updater = AsyncDataUpdater(pool=pool,
                               binance_api_key=ConfigBinance.BINANCE_API_KEY,
                               binance_api_secret=ConfigBinance.BINANCE_API_SECRET,
                               )

    print("Drop duplicates. Success: ", await updater.drop_duplicates_rows(table_name="spot_data_btcusdt_1m"))

    """ Start testing get_data_as_df method """
    start_datetime = datetime.datetime.strptime('01 Aug 2017', '%d %b %Y').replace(tzinfo=timezone.utc)
    _data_df = await updater.get_data_as_df(table_name="spot_data_btcusdt_1m",
                                            start=start_datetime,
                                            end=datetime.datetime.now(timezone.utc),
                                            use_cols=Constants.ohlcv_cols,
                                            use_dtypes=Constants.ohlcv_dtypes
                                            )
    _data_df['open_time'] = pd.to_datetime(_data_df['open_time'], unit='ms')
    _data_df['open_time'] = pd.to_datetime(_data_df['open_time'],
                                           infer_datetime_format=True,
                                           format=Constants.default_datetime_format)

    _data_df['open_time'] = DataRepair.convert_timestamp_to_datetime(_data_df['open_time'])
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
    asyncio.run(main())
