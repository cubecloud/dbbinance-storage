import logging
from config.configbinance import ConfigBinance
from config.configpostgresql import ConfigPostgreSQL
from fetcher.datafetcher import DataUpdater

version = 0.0013

logger = logging.getLogger()

logger.setLevel(logging.INFO)

file_handler = logging.FileHandler('dbdata_updater.log')
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

logging.getLogger('apscheduler').setLevel(logging.INFO)

updater = DataUpdater(host=ConfigPostgreSQL.HOST,
                      database=ConfigPostgreSQL.DATABASE,
                      user=ConfigPostgreSQL.USER,
                      password=ConfigPostgreSQL.PASSWORD,
                      binance_api_key=ConfigBinance.BINANCE_API_KEY,
                      binance_api_secret=ConfigBinance.BINANCE_API_SECRET,
                      symbol_pairs=['BTCUSDT',]
                      )
updater.test_background_updater(60)
