from typing import Optional
from dbbinance.config.configpostgresql import ConfigPostgreSQL
from dbbinance.fetcher.asyncsqlbase import create_pool
from dbbinance.fetcher.asyncdatafetcher import AsyncDataFetcher
from dbbinance.fetcher.fetchercachemanager import FetcherCacheManager

__version__ = 0.007


async def get_async_datafetcher(cache_obj: Optional[FetcherCacheManager] = None, port=5432, min_size=1,
                                max_size=12) -> AsyncDataFetcher:
    pool = await create_pool(host=ConfigPostgreSQL.HOST,
                             database=ConfigPostgreSQL.DATABASE,
                             user=ConfigPostgreSQL.USER,
                             password=ConfigPostgreSQL.PASSWORD,
                             port=5432,
                             min_size=min_size,
                             max_size=max_size
                             )

    async_fetcher = AsyncDataFetcher(pool=pool,
                                     binance_api_key="dummy",
                                     binance_api_secret="dummy",
                                     cache_obj=cache_obj,
                                     )
    return async_fetcher
