import sys
import logging
from logging.handlers import TimedRotatingFileHandler
from dbbinance.config import ConfigBinance
from dbbinance.config import ConfigPostgreSQL
from dbbinance.fetcher.datafetcher import DataUpdater
from dbbinance.config.dockerized import is_pythonunbuffered

version = 0.80

logger = logging.getLogger()

logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(processName)s - %(name)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler(sys.stdout)
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
