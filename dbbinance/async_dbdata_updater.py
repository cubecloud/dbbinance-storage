import logging
import asyncio
from logging.handlers import TimedRotatingFileHandler
from dbbinance.config.configbinance import ConfigBinance
from dbbinance.config.configpostgresql import ConfigPostgreSQL
from dbbinance.fetcher.asyncdatafetcher import AsyncDataUpdater
from dbbinance.fetcher.asyncsqlbase import create_pool

version = 0.86


async def main():
    pool = await create_pool(host=ConfigPostgreSQL.HOST,
                             database=ConfigPostgreSQL.DATABASE,
                             user=ConfigPostgreSQL.USER,
                             password=ConfigPostgreSQL.PASSWORD)

    updater = AsyncDataUpdater(pool=pool,
                               binance_api_key=ConfigBinance.BINANCE_API_KEY,
                               binance_api_secret=ConfigBinance.BINANCE_API_SECRET,
                               symbol_pairs=['BTCUSDT', ]
                               )
    await updater.start_background_updater(60)

if __name__ == "__main__":
    logger = logging.getLogger()

    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # file_handler = TimedRotatingFileHandler('dbdata_updater.log', when='D', interval=3, backupCount=3)
    # file_handler.setLevel(logging.INFO)
    # file_handler.setFormatter(formatter)
    # logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    logging.getLogger('apscheduler').setLevel(logging.INFO)
    asyncio.run(main())
