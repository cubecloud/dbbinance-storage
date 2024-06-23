import os
import time
import pytz
import logging
import datetime
import pandas as pd
from typing import Tuple, Union
from datetime import timezone

from pandas import Timestamp

from fetcher.constants import Constants
from fetcher.sqlbase import SQLMeta, handle_errors
from fetcher.cachemanager import CacheManager
from fetcher.datautils import convert_timeframe_to_freq
from collections import OrderedDict
from dotenv import load_dotenv, find_dotenv

from binance.client import Client
from binance.exceptions import BinanceAPIException

from apscheduler.schedulers.background import BackgroundScheduler

__version__ = 0.032

logger = logging.getLogger()

load_dotenv(find_dotenv())


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


class PostgreSQLDatabase(SQLMeta):
    def __init__(self, host, database, user, password, max_conn=10):
        super().__init__(host, database, user, password, max_conn)
        # self.conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)
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

    @handle_errors
    def is_data_exists(self, table_name):
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {table_name} LIMIT 1")
            _data = cur.fetchone()
        if _data is None:
            data_exists = False
        else:
            data_exists = True
        return data_exists

    @handle_errors
    def create_table(self, table_name="spot_data"):
        # Start a transaction
        if not self.is_table_exists(table_name):
            conn = self.conn  # Used for optimizing calls of @property
            with conn.cursor() as cur:
                query = f"CREATE TABLE {table_name} (id SERIAL PRIMARY KEY, open_time BIGINT, open NUMERIC, high NUMERIC," \
                        f" low NUMERIC, close NUMERIC, volume NUMERIC, close_time BIGINT, quote_asset_volume NUMERIC, " \
                        f"trades INTEGER, taker_buy_base NUMERIC, taker_buy_quote NUMERIC, ignored NUMERIC)"
                cur.execute(query)
            conn.commit()
            logger.debug(f"{self.__class__.__name__}: Created table '{table_name}'")

    # @handle_errors
    def insert_kline_to_table(self, table_name, kline):
        # Start a transaction
        conn = self.conn  # Used for optimizing calls of @property
        with conn.cursor() as cur:
            cur.execute("BEGIN;")
            cur.execute(f"LOCK TABLE {table_name} IN ACCESS EXCLUSIVE MODE;")

            open_time, open_price, high_price, low_price, close_price, volume, close_time, quote_asset_volume, \
                trades, taker_buy_base, taker_buy_quote, ignored = kline[:12]
            query = f"INSERT INTO {table_name} (open_time, open, high, low, close, volume, close_time, " \
                    f"quote_asset_volume, trades, taker_buy_base, taker_buy_quote, ignored) " \
                    f"VALUES ('{open_time}', '{open_price}', '{high_price}', '{low_price}', '{close_price}', " \
                    f"'{volume}', '{close_time}', '{quote_asset_volume}', '{trades}', '{taker_buy_base}', " \
                    f"'{taker_buy_quote}', '{ignored}')"
            cur.execute(query)
        conn.commit()
        conn.close()

    # @handle_errors
    def insert_klines_to_table(self, table_name, klines):
        # Start a transaction
        conn = self.conn  # Used for optimizing calls of @property
        with conn.cursor() as cur:
            cur.execute("BEGIN;")
            cur.execute(f"LOCK TABLE {table_name} IN ACCESS EXCLUSIVE MODE;")

            query = f"INSERT INTO {table_name} (open_time, open, high, low, close, volume, close_time, " \
                    f"quote_asset_volume, trades, taker_buy_base, taker_buy_quote, ignored) " \
                    f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            cur.executemany(query, klines)
        conn.commit()

    def logger_debug(self, msg):
        if self.max_verbose:
            logger.debug(msg)

    @handle_errors
    def get_min_open_time(self, table_name, retry=10) -> int:
        self.logger_debug(
            f"{self.__class__.__name__}: get_min_open_time called with table_name={table_name} and retry={retry}")
        conn = self.conn  # Used for optimizing calls of @property
        try:
            with conn.cursor() as cur:
                query = f"SELECT MIN(open_time) FROM {table_name}"
                self.logger_debug(f"{self.__class__.__name__}: Executing query: {query}")
                cur.execute(query)
                if cur.rowcount != 0:
                    min_open_time = cur.fetchone()[0]
                    self.logger_debug(f"{self.__class__.__name__}:Query returned min_open_time={min_open_time}")
                else:
                    self.logger_debug(f"{self.__class__.__name__}: Query returned no rows")
                    if retry > 0:
                        self.logger_debug(f"{self.__class__.__name__}: Retrying with retry={retry - 1}")
                        min_open_time = self.get_min_open_time(table_name, retry - 1)
                    else:
                        self.logger_debug(f"{self.__class__.__name__}: No more retries left")
                        min_open_time = None
        except Exception as e:
            conn.rollback()
            logger.error(f"{self.__class__.__name__}: An error occurred while executing the query: {e}")
            min_open_time = None

        if (not min_open_time) or (min_open_time is None):
            min_open_time = datetime.datetime.strptime('01 Jan 2017', '%d %b %Y')
            min_open_time = int(datetime.datetime.timestamp(min_open_time) * 1000)
            self.logger_debug(f"{self.__class__.__name__}: Returning default value: {min_open_time}")
        else:
            self.logger_debug(f"{self.__class__.__name__}: Returning min_open_time: {min_open_time}")
        return min_open_time

    @handle_errors
    def get_max_open_time(self, table_name, retry=10) -> int:
        self.logger_debug(
            f"{self.__class__.__name__}:get_max_open_time called with table_name={table_name} and retry={retry}")
        conn = self.conn  # Used for optimizing calls of @property
        try:
            with conn.cursor() as cur:
                query = f"SELECT MAX(open_time) FROM {table_name}"
                self.logger_debug(f"{self.__class__.__name__}: Executing query: {query}")
                cur.execute(query)
                if cur.rowcount != 0:
                    max_open_time = cur.fetchone()[0]
                    self.logger_debug(f"{self.__class__.__name__}: Query returned max_open_time={max_open_time}")
                else:
                    self.logger_debug(f"{self.__class__.__name__}: Query returned no rows")
                    if retry > 0:
                        self.logger_debug(f"{self.__class__.__name__}: Retrying with retry={retry - 1}")
                        max_open_time = self.get_min_open_time(table_name, retry - 1)
                    else:
                        self.logger_debug(f"{self.__class__.__name__}: No more retries left")
                        max_open_time = None
        except Exception as e:
            conn.rollback()
            logger.error(f"{self.__class__.__name__}: An error occurred while executing the query: {e}")
            max_open_time = None
        if (not max_open_time) or (max_open_time is None):
            max_open_time = datetime.datetime.now(timezone.utc)
            max_open_time = int(datetime.datetime.timestamp(max_open_time) * 1000)
            self.logger_debug(f"{self.__class__.__name__}: Returning current timestamp: {max_open_time}")
        else:
            self.logger_debug(f"{self.__class__.__name__}: Returning max_open_time: {max_open_time}")
        return max_open_time

    @handle_errors
    def is_duplicates_exists(self, table_name, show=True) -> bool:
        with self.conn.cursor() as cur:
            query = f"""
            SELECT open_time, COUNT(*)
            FROM {table_name}
            GROUP BY open_time
            HAVING COUNT(*) > 1;
            """
            cur.execute(query)
            duplicates = cur.fetchall()
        status = False
        if duplicates:
            status = True
            if show:
                logger.warning(f"{self.__class__.__name__}: Duplicates:")
                for row in duplicates:
                    logger.warning(f"{self.__class__.__name__}: {row}")
        return status

    @handle_errors
    def drop_duplicates_rows(self, table_name):
        conn = self.conn
        with conn.cursor() as cur:
            query = f"""
            DELETE FROM {table_name}
            WHERE id IN (
                SELECT id
                FROM (
                    SELECT id,
                    ROW_NUMBER() OVER( PARTITION BY open_time ORDER BY id ) AS row_num
                    FROM {table_name}
                ) t
                WHERE t.row_num > 1
            );
            """
            cur.execute(query)
        conn.commit()
        conn.close()


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

    def get_kline_period(self, start_point, end_point, symbol_pair, timeframe) -> list:
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


class DataUpdaterMeta(PostgreSQLDatabase):
    updater_is_running = updater_status

    def __init__(self, host, database, user, password, binance_api_key, binance_api_secret,
                 symbol_pairs=None, timeframes=None):
        super().__init__(host, database, user, password)
        # logger.debug from parent class

        if symbol_pairs is None:
            self.symbol_pairs: list = ['BTCUSDT']
        if timeframes is None:
            self.timeframes: list = ['1m']

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

    def prepare_start_end(self,
                          table_name: str,
                          start: datetime.datetime or int or str or None,
                          end: datetime.datetime or int or str or None) -> Tuple[int, int]:

        if isinstance(start, int):
            start_timestamp = int(start)
        elif isinstance(start, str):
            start_timestamp = datetime.datetime.strptime(start,
                                                         Constants.default_datetime_format).replace(tzinfo=pytz.utc)
            start_timestamp = int(start_timestamp.timestamp() * 1000)
        else:
            start_timestamp = int(start.timestamp() * 1000)

        if end is None:
            end_timestamp = int(self.get_max_open_time(table_name))
        elif isinstance(end, int):
            end_timestamp = int(end)
        elif isinstance(end, str):
            end_timestamp = datetime.datetime.strptime(end,
                                                       Constants.default_datetime_format).replace(tzinfo=pytz.utc)
            end_timestamp = int(end_timestamp.timestamp() * 1000)
        else:
            end_timestamp = int(end.timestamp() * 1000)

        start_open_time = int(self.get_min_open_time(table_name))

        if start_timestamp < start_open_time or start_timestamp is None:
            start_timestamp = start_open_time

        return start_timestamp, end_timestamp


class DataUpdater(DataUpdaterMeta):
    updater_is_running = updater_status

    def __init__(self, host, database, user, password, binance_api_key, binance_api_secret,
                 symbol_pairs=None, timeframes=None):
        super().__init__(host, database, user, password, binance_api_key, binance_api_secret, symbol_pairs, timeframes)

    def check_first_run(self):
        """ Check if this a first run """
        msg = f"Checking the 1st run"
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
                        self.is_duplicates_exists(table_name)
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
                             f"start: {datetime.datetime.utcfromtimestamp(klines[0][0] / 1000).replace(tzinfo=pytz.utc)}, "
                             f"end: {datetime.datetime.utcfromtimestamp(klines[-1][0] / 1000).replace(tzinfo=pytz.utc)}")

        start_time = datetime.datetime.now(timezone.utc)
        logger.debug(f"{self.__class__.__name__} #{self.idnum}: Start saving historical data to database for "
                     f"{symbol_pair}, {timeframe} at: {start_time}")

        self.insert_klines_to_table(table_name, klines)

        logger.debug(f"{self.__class__.__name__} #{self.idnum}: Finished saving historical data to database for "
                     f"{symbol_pair}, {timeframe} at: {datetime.datetime.now(timezone.utc)}. "
                     f"ETA: {datetime.datetime.now(timezone.utc) - start_time}")

    @staticmethod
    def convert_timeframe_to_min(timeframe):
        if timeframe[-1:] == 'h' or timeframe[-1:] == 'H':
            min_size = 60
        elif timeframe[-1:] == 'm':
            min_size = 1
        elif timeframe[-1:] == 'd' or timeframe[-1:] == 'D':
            min_size = 24 * 60
        elif timeframe[-1:] == 'w' or timeframe[-1:] == 'w':
            min_size = 7 * 24 * 60
        converted_data = min_size * int(timeframe[:-1])
        return converted_data

    def update_spot_data(self):
        logger.debug(f"{self.__class__.__name__} #{self.idnum}: Update started...")
        # self.cur = self.conn.cursor()
        for base_table_name in self.base_tables_names:
            for symbol_pair in self.symbol_pairs:
                for timeframe in self.timeframes:
                    table_name = f"{base_table_name}_{symbol_pair}_{timeframe}".lower()
                    start_open_time = self.get_max_open_time(table_name)
                    start_open_time = datetime.datetime.utcfromtimestamp(start_open_time / 1000).replace(
                        tzinfo=pytz.utc)
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
                            self.last_timeframe_datetime = datetime.datetime.utcfromtimestamp(
                                klines[-1][0] / 1000).replace(tzinfo=pytz.utc)
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
                       start: datetime.datetime or int,
                       end: datetime.datetime or int,
                       use_cols=Constants.sql_cols,
                       use_dtypes=None,
                       ) -> pd.DataFrame:

        _start_time = datetime.datetime.now(timezone.utc)
        if use_dtypes is None:
            use_dtypes = Constants.sql_dtypes

        start_timestamp, end_timestamp = self.prepare_start_end(table_name, start, end)

        with self.conn.cursor() as cur:
            query = f"SELECT {', '.join(use_cols)} FROM {table_name} " \
                    f"WHERE open_time BETWEEN '{start_timestamp}' AND '{end_timestamp}' ORDER BY open_time"
            logger.debug(f"{self.__class__.__name__} #{self.idnum}: "
                         f"Start getting historical data from database table {table_name} at: {_start_time}")
            cur.execute(query)
            data = cur.fetchall()
            logger.debug(f"{self.__class__.__name__} #{self.idnum}: "
                         f"Finished getting historical data from database table {table_name} at: "
                         f"{datetime.datetime.now(timezone.utc)}. "
                         f"ETA: {datetime.datetime.now(timezone.utc) - _start_time}")
        data_df = pd.DataFrame(data, columns=list(use_cols))
        data_df = data_df.astype(use_dtypes)
        return data_df

    def is_symbol_pair_available(self, market, symbol_pair) -> list:
        base_table_name = f"{market}_data"
        timeframe = "1m"
        table_name = f"{base_table_name}_{symbol_pair}_{timeframe}".lower()
        status = self.is_table_exists(table_name)
        return status

    def get_all_data_as_df(self, table_name, use_cols=Constants.sql_cols, use_dtypes=None) -> pd.DataFrame:
        if use_dtypes is None:
            use_dtypes = Constants.sql_dtypes

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
            data_df = data_df.drop(columns="id")
            self.insert_klines_to_table(table_name, data_df.to_numpy())
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

            back_start_time = ceil_time(datetime.datetime.now(), "1m")
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


cache_manager_obj = CacheManager(max_memory_gb=2)


class DataFetcher(DataUpdaterMeta):
    CM = cache_manager_obj

    def __init__(self, host, database, user, password, binance_api_key, binance_api_secret):
        super().__init__(host, database, user, password, binance_api_key, binance_api_secret)

    def _get_agg_dict(self, use_cols):
        actual_agg_dict = OrderedDict()
        for col_name in use_cols:
            if col_name != "open_time":
                actual_agg_dict.update({col_name: self.default_agg_dict.get(col_name, None)})
        return actual_agg_dict

    def resample_to_timeframe(self,
                              table_name: str,
                              start: datetime.datetime or int,
                              end: datetime.datetime or int,
                              to_timeframe: str = "1h",
                              origin: str = "start",
                              use_cols: tuple = Constants.binance_cols,
                              use_dtypes=None,
                              open_time_index: bool = True,
                              cached=False,
                              ) -> pd.DataFrame:

        def fetch_raw_data():
            conn = self.get_conn()
            conn.set_session(readonly=True, autocommit=True)
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT {', '.join(use_cols)} "
                    f"FROM {table_name} "
                    f"WHERE open_time >= {start_timestamp} AND open_time <= {end_timestamp}")
                raw_data = cur.fetchall()
            conn.close()
            return raw_data

        def prepare_raw_df():
            _df = pd.DataFrame(fetch_raw_data(), columns=list(use_cols))
            _df = _df.astype(use_dtypes)
            _df = _df.sort_values(by=['open_time']).set_index('open_time', inplace=False, drop=False)
            return _df

        def get_raw_df():
            raw_df = None
            """ Cached raw df (minutes) """
            if cached:
                cache_key = self.CM.get_cache_key(table_name=table_name, start_timestamp=start_timestamp,
                                                  end_timestamp=end_timestamp)

                #   check if data is already in
                if cache_key in self.CM.cache.keys():
                    raw_df = self.CM.cache[cache_key]
                    logger.debug(
                        f"{self.__class__.__name__} #{self.idnum}: Return cached RAW data: {table_name}/ {start_timestamp} - {end_timestamp}")
                else:
                    for key in self.CM.cache.keys():
                        if (len(key) == 3) and (key[2][1] == table_name) and (start_timestamp >= key[1][1]) and (
                                end_timestamp <= key[0][1]):
                            raw_df = self.CM.cache[key].loc[start_timestamp:end_timestamp]
                            logger.debug(
                                f"{self.__class__.__name__} #{self.idnum}: Return cached RAW data: {table_name} / {raw_df.index[0]} - {raw_df.index[-1]}")
                            break
                    if raw_df is None:
                        raw_df = prepare_raw_df()
                        self.CM.update_cache(key=cache_key, value=raw_df.copy(deep=True))
            else:
                raw_df = prepare_raw_df()
            return raw_df.copy(deep=True)

        def prepare_resampled_df():
            resampled_df = get_raw_df()

            resampled_df['open_time'] = DataRepair.convert_timestamp_to_datetime(resampled_df['open_time'])
            resampled_df = resampled_df.set_index('open_time', inplace=False)

            agg_dict = self._get_agg_dict(use_cols)
            freq = convert_timeframe_to_freq(to_timeframe)

            resampled_df = resampled_df.resample(freq, label='right', closed='right', origin=origin).agg(agg_dict)
            if not open_time_index:
                resampled_df = resampled_df.reset_index()

            logger.debug(f"{self.__class__.__name__} #{self.idnum}: "
                         f"'{table_name}' resample to timeframe: '{freq}', "
                         f"from origin: {origin}, start-end: "
                         f"{DataRepair.convert_timestamp_to_datetime(start_timestamp)} - "
                         f"{DataRepair.convert_timestamp_to_datetime(end_timestamp)}")
            return resampled_df

        def get_resampled_df():
            if cached:
                cache_key = self.CM.get_cache_key(table_name=table_name, start_timestamp=start_timestamp,
                                                  end_timestamp=end_timestamp, timeframe=to_timeframe, origin=origin,
                                                  open_time_index=open_time_index)

                if cache_key in self.CM.cache.keys():
                    resampled_df = self.CM.cache[cache_key]
                    logger.debug(
                        f"{self.__class__.__name__} #{self.idnum}: Return cached RESAMPLED data: {table_name} / {start_timestamp} - {end_timestamp}")
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


if __name__ == "__main__":
    from config.configpostgresql import ConfigPostgreSQL

    logger.setLevel(logging.DEBUG)

    file_handler = logging.FileHandler('mylog.log')
    file_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    logging.getLogger('apscheduler').setLevel(logging.DEBUG)

    from secure_apikey.secure import Secure

    """ Decrypt binance api key and binance api secret """
    # secure_key = Secure()
    # _binance_api_key, _binance_api_secret = secure_key.get_key()

    # updater = DataUpdater(host=ConfigPostgreSQL.HOST,
    #                       database=ConfigPostgreSQL.DATABASE,
    #                       user=ConfigPostgreSQL.USER,
    #                       password=ConfigPostgreSQL.PASSWORD,
    #                       binance_api_key=_binance_api_key,
    #                       binance_api_secret=_binance_api_secret,
    #                       )
    # updater.test_background_updater()
    # updater.drop_duplicates_rows(table_name="spot_data_btcusdt_1m")

    # """ Start testing get_data_as_df method """
    # start_datetime = datetime.datetime.strptime('01 Aug 2017', '%d %b %Y').replace(tzinfo=timezone.utc)
    # _data_df = updater.get_data_as_df(table_name="spot_data_btcusdt_1m",
    #                                   start=start_datetime,
    #                                   end=datetime.datetime.now(timezone.utc),
    #                                   use_cols=Constants.ohlcv_cols,
    #                                   use_dtypes=Constants.ohlcv_dtypes
    #                                   )
    # _data_df['open_time'] = pd.to_datetime(_data_df['open_time'], unit='ms')
    # _data_df['open_time'] = pd.to_datetime(_data_df['open_time'],
    #                                        infer_datetime_format=True,
    #                                        format=Constants.default_datetime_format)
    #
    # _data_df['open_time'] = DataRepair.convert_timestamp_to_datetime(_data_df['open_time'])
    # _data_df = _data_df.set_index('open_time', inplace=False)
    #
    # print(_data_df.head(15).to_string())
    # print(_data_df.tail(15).to_string())
    # """ End testing get_data_as_df method """

    """ Start testing resample_to_timeframe method and cached option"""
    fetcher = DataFetcher(host=ConfigPostgreSQL.HOST,
                          database=ConfigPostgreSQL.DATABASE,
                          user=ConfigPostgreSQL.USER,
                          password=ConfigPostgreSQL.PASSWORD,
                          binance_api_key='dummy',
                          binance_api_secret='dummy',
                          )

    start_datetime = datetime.datetime.strptime('01 Aug 2018', '%d %b %Y').replace(tzinfo=timezone.utc)
    end_datetime = datetime.datetime.now(timezone.utc)
    # end_datetime = datetime.datetime.now(timezone.utc) - datetime.timedelta(minutes=1)
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
    print(_data_df.compare(_data_df_1, align_axis=0).to_string())
    fetcher2 = DataFetcher(host=ConfigPostgreSQL.HOST,
                           database=ConfigPostgreSQL.DATABASE,
                           user=ConfigPostgreSQL.USER,
                           password=ConfigPostgreSQL.PASSWORD,
                           binance_api_key='dummy',
                           binance_api_secret='dummy',
                           )
    print(start_datetime, end_datetime)
    end_datetime = datetime.datetime.now(timezone.utc) - datetime.timedelta(minutes=1)
    # end_datetime = datetime.datetime.now(timezone.utc)
    print(start_datetime, end_datetime)
    _data_df_3 = fetcher2.resample_to_timeframe(table_name="spot_data_btcusdt_1m",
                                                start=start_datetime,
                                                end=end_datetime,
                                                to_timeframe="30m",
                                                origin="start",
                                                use_cols=Constants.ohlcv_cols,
                                                use_dtypes=Constants.ohlcv_dtypes,
                                                open_time_index=False,
                                                cached=True)
    print(_data_df_3.head(2).to_string())
    print(_data_df_3.tail(1).to_string())
    # print(_data_df.compare(_data_df_3, align_axis=0).to_string())
    """ End testing resample_to_timeframe method """
