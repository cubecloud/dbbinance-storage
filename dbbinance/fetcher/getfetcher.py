from dbbinance.config.configpostgresql import ConfigPostgreSQL
from dbbinance.fetcher.datafetcher import DataFetcher

__version__ = 0.002


def get_datafetcher(host=ConfigPostgreSQL.HOST,
                    database=ConfigPostgreSQL.DATABASE,
                    user=ConfigPostgreSQL.USER,
                    password=ConfigPostgreSQL.PASSWORD) -> DataFetcher:
    """ Decrypt binance api key and binance api secret """

    psg_kwargs = dict(host=host,
                      database=database,
                      user=user,
                      password=password)

    """ DataFetcher object (read-only) to get data from database for using with models """
    dummy_kwargs = dict(binance_api_key="dummy",
                        binance_api_secret="dummy")

    fetcher_obj_kwargs = dict(**psg_kwargs,
                              **dummy_kwargs)

    data_fetcher_obj = DataFetcher(**fetcher_obj_kwargs)

    return data_fetcher_obj
