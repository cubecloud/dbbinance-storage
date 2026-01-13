import logging
import asyncpg
from typing import Optional

__version__ = 0.022

logger = logging.getLogger()

POOL: Optional[asyncpg.Pool] = None


# noinspection PyUnresolvedReferences
async def create_global_pool(database=None, user=None, password=None, host='localhost', port=5432, min_size=1,
                             max_size=12):
    global POOL
    POOL = await create_pool(database=database, user=user, password=password, host=host, port=port,
                             min_size=min_size, max_size=max_size)


# noinspection PyUnresolvedReferences
async def create_pool(database=None, user=None, password=None, host='localhost', port=5432, min_size=1,
                      max_size=12):
    pool = await asyncpg.create_pool(database=database, user=user, password=password, host=host, port=port,
                                     min_size=min_size, max_size=max_size)

    return pool


class AsyncPool:
    """Class managing database connections through a connection pool."""

    def __init__(self, pool: Optional[asyncpg.Pool] = None):
        # Create a new connection pool instance
        global POOL
        if pool is None:
            self.pool = POOL
        else:
            self.pool = pool
        self.conn = None

    # noinspection PyUnresolvedReferences
    async def __aenter__(self):
        """
        Enter the runtime context related to this object.
        Acquires a connection from the pool when entering a `async with` block.

        Returns:
          A connection object retrieved from the pool.
        """
        self.conn = await self.pool.acquire()
        return self.conn

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the runtime context related to this object.
        Releases the connection back into the pool when leaving a `async with` block.
        """
        if self.conn is not None:
            await self.pool.release(self.conn)
