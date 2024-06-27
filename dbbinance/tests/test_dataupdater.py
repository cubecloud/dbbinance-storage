import logging
import datetime
import pandas as pd
from datetime import timezone

from dbbinance.fetcher.constants import Constants
from dbbinance.fetcher.datafetcher import DataRepair
from dbbinance.fetcher.datafetcher import DataUpdater
from dbbinance.config.configpostgresql import ConfigPostgreSQL
from dbbinance.config.configbinance import ConfigBinance

logger = logging.getLogger()


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

""" Decrypt binance api key and binance api secret """
updater = DataUpdater(host=ConfigPostgreSQL.HOST,
                      database=ConfigPostgreSQL.DATABASE,
                      user=ConfigPostgreSQL.USER,
                      password=ConfigPostgreSQL.PASSWORD,
                      binance_api_key=ConfigBinance.BINANCE_API_KEY,
                      binance_api_secret=ConfigBinance.BINANCE_API_SECRET,
                      )
# updater.test_background_updater()
updater.drop_duplicates_rows(table_name="spot_data_btcusdt_1m")

""" Start testing get_data_as_df method """
start_datetime = datetime.datetime.strptime('01 Aug 2017', '%d %b %Y').replace(tzinfo=timezone.utc)
_data_df = updater.get_data_as_df(table_name="spot_data_btcusdt_1m",
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

logger.debug(f"data_df head: \n{_data_df.head(15).to_string()}\n")
logger.debug(f"data_df tail: \n{_data_df.tail(15).to_string()}\n")
""" End testing get_data_as_df method """

