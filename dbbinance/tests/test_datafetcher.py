import logging
import datetime
from datetime import timezone

from dbbinance.fetcher.constants import Constants
from dbbinance.fetcher.datafetcher import DataFetcher
from dbbinance.fetcher.datafetcher import ceil_time, floor_time

from dbbinance.config.configpostgresql import ConfigPostgreSQL

logger = logging.getLogger()


logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler('test_datafetcher.log')
file_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

logging.getLogger('apscheduler').setLevel(logging.DEBUG)

""" Start testing resample_to_timeframe method and 'cached' option"""
fetcher = DataFetcher(host=ConfigPostgreSQL.HOST,
                      database=ConfigPostgreSQL.DATABASE,
                      user=ConfigPostgreSQL.USER,
                      password=ConfigPostgreSQL.PASSWORD,
                      binance_api_key='dummy',
                      binance_api_secret='dummy',
                      )

logger.debug("\nResampling of same period and compare results with 'cached=True' option\n")
start_datetime = datetime.datetime.strptime('01 Aug 2018', '%d %b %Y').replace(tzinfo=timezone.utc)
""" we need a gap for testing """
end_datetime = floor_time(datetime.datetime.now(timezone.utc) - datetime.timedelta(minutes=1))
logger.debug(f'Start datetime - end datetime: {start_datetime} - {end_datetime}\n')
_data_df = fetcher.resample_to_timeframe(table_name="spot_data_btcusdt_1m",
                                         start=start_datetime,
                                         end=end_datetime,
                                         to_timeframe="1h",
                                         origin="start",
                                         use_cols=Constants.ohlcv_cols,
                                         use_dtypes=Constants.ohlcv_dtypes,
                                         open_time_index=False,
                                         cached=True)
logger.debug(f"data_df head: \n{_data_df.head(15).to_string()}\n")
logger.debug(f"data_df tail: \n{_data_df.tail(15).to_string()}\n")
_data_df_1 = fetcher.resample_to_timeframe(table_name="spot_data_btcusdt_1m",
                                           start=start_datetime,
                                           end=end_datetime,
                                           to_timeframe="1h",
                                           origin="start",
                                           use_cols=Constants.ohlcv_cols,
                                           use_dtypes=Constants.ohlcv_dtypes,
                                           open_time_index=False,
                                           cached=True)
logger.debug(f"data_df_1 head: \n{_data_df_1.head(15).to_string()}\n")
logger.debug(f"data_df_1 tail: \n{_data_df_1.tail(15).to_string()}\n")
logger.debug("\nCompare result:\n")
logger.debug(_data_df.compare(_data_df_1, align_axis=0).to_string())
fetcher2 = DataFetcher(host=ConfigPostgreSQL.HOST,
                       database=ConfigPostgreSQL.DATABASE,
                       user=ConfigPostgreSQL.USER,
                       password=ConfigPostgreSQL.PASSWORD,
                       binance_api_key='dummy',
                       binance_api_secret='dummy',
                       )
logger.debug(f'Start datetime - end datetime: {start_datetime} - {end_datetime}\n')
logger.debug("\nResampling of same period and compare results with 'cached=False' option\n")
logger.debug(f'Start datetime - end datetime: {start_datetime} - {end_datetime}')
_data_df_3 = fetcher2.resample_to_timeframe(table_name="spot_data_btcusdt_1m",
                                            start=start_datetime,
                                            end=end_datetime,
                                            to_timeframe="1h",
                                            origin="start",
                                            use_cols=Constants.ohlcv_cols,
                                            use_dtypes=Constants.ohlcv_dtypes,
                                            open_time_index=False,
                                            cached=False)
logger.debug(f"data_df_3 head: \n{_data_df_3.head(15).to_string()}\n")
logger.debug(f"data_df_3 tail: \n{_data_df_3.tail(15).to_string()}\n")
logger.debug("\nCompare result:\n")
logger.debug(_data_df.compare(_data_df_3, align_axis=0).to_string())
""" End testing resample_to_timeframe method """
