import logging
import asyncio
import asyncpg
from typing import List, Tuple, Optional
from collections import OrderedDict
from dbbinance.config.configpostgresql import ConfigPostgreSQL
from dbbinance.fetcher import AsyncPool
from dbbinance.fetcher import create_pool

__version__ = 0.022

logger = logging.getLogger()

_pool: Optional[asyncpg.Pool] = None


class AsyncDBConnectionManager:
    def __init__(self, pool: Optional[asyncpg.Pool] = None):
        """
        Initializes the connection pool with provided configuration settings.
        """
        if pool is None:
            raise f"Error: pool is not initialized"
        self.pool = pool

    async def modify_query(self, query: str, params: Optional[Tuple] = None) -> bool:
        """
        Execute a modifying query (CREATE, INSERT, UPDATE, DELETE) without fetching results.

        Args:
            query (sql.SQL | str): SQL query to execute.
            params (tuple|list, optional): Parameters to substitute into the query.
        """
        try:
            async with AsyncPool(self.pool) as conn:
                await conn.execute(query, *params if params else ())
            return True
        except Exception as ex:
            logger.debug(f"{self.__class__.__name__}: Database error in modify_query: {ex}")
            raise
        finally:
            # asyncpg handles connection release automatically
            pass

    async def single_select_query(self, query: str, params: Optional[tuple] = None) -> Optional[Tuple]:
        """
        Execute a SELECT query expecting exactly one row and return it.

        Args:
            query (str): SQL query to execute.
            params (tuple, optional): Parameters to substitute into the query.

        Returns:
            tuple or None: Single row returned by the query execution.
        """
        try:
            async with AsyncPool(self.pool) as conn:
                result = await conn.fetchrow(query, *params if params else ())
                return result
        except Exception as e:
            logger.error(f"{self.__class__.__name__}: Database error in single_select_query: {e}")
            raise
        finally:
            # asyncpg handles connection release automatically
            pass

    async def select_query(self, query: str, params: Optional[tuple] = None) -> Optional[List]:
        """
        Execute a SELECT query and return its result.

        Args:
            query (str): SQL query to execute.
            params (tuple, optional): Parameters to substitute into the query.

        Returns:
            list[tuple, optional]: Result set returned by the query execution.
        """
        try:
            async with AsyncPool(self.pool) as conn:
                result = await conn.fetch(query, *params if params else ())
                return result
        except Exception as e:
            logger.error(f"{self.__class__.__name__}: Database error in select_query: {e}")
            raise
        finally:
            # asyncpg handles connection release automatically
            pass


class AsyncSQLMeta:
    count = 0

    def __init__(self, pool):
        AsyncSQLMeta.count += 1
        self.idnum = int(AsyncSQLMeta.count)
        self.pool = pool
        self.asyncdb_mgr = AsyncDBConnectionManager(self.pool)
        self.__connections = OrderedDict()  # Will be Deprecated in future

        """ Get external data with logger """
        logger.debug(f"{self.__class__.__name__} #{self.idnum}': Initializing...")

    def __del__(self):
        AsyncSQLMeta.count -= 1

    async def is_table_exists(self, table_name: str) -> bool:
        """
        Check the table existence in database

        Args:
            table_name (str):   table name

        Returns:
            bool: True, if table exist, False, if NOT exist.
        """
        try:
            async with AsyncPool(self.pool) as conn:
                query = """
                       SELECT EXISTS (
                           SELECT FROM information_schema.tables 
                           WHERE table_name = $1
                       );
                   """
                result = await conn.fetchval(query, table_name)
                logger.debug(f"Checked existence of table '{table_name}': {result}")
                return bool(result)
        except Exception as e:
            logger.error(f"{self.__class__.__name__}: Database error in is_table_exists: {e}")
            raise
        finally:
            # asyncpg handles connection release automatically
            pass

    async def drop_table(self, table_name: str) -> bool:
        """
        Drop a table from the database.

        Args:
            table_name (str): Name of the table to drop.

        Returns:
            bool: True if the operation succeeded, False otherwise.
        """
        try:
            async with AsyncPool(self.pool) as conn:
                # Safely drop the table
                await conn.execute("DROP TABLE IF EXISTS $1 CASCADE;", table_name)
            logger.debug(f"{self.__class__.__name__}: DROP TABLE '{table_name}'")
            return True
        except Exception as e:
            logger.error(f"{self.__class__.__name__}: Failed to drop table '{table_name}': {e}")
            return False
        finally:
            # asyncpg handles connection release automatically
            pass

    async def get_tables_list(self) -> List:
        """
        Retrieve a list of public schema's base tables from the database.

        Returns:
            list: List of table names.
        """
        try:
            async with AsyncPool(self.pool) as conn:
                query = """
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_type = 'BASE TABLE' 
                      AND table_schema = 'public'
                """
                tables = await conn.fetch(query)
                table_names = [row['table_name'] for row in tables]
                logger.debug(f"{self.__class__.__name__}: Retrieved list of tables: {table_names}")
                return table_names
        except Exception as e:
            logger.error(f"{self.__class__.__name__}: Database error in get_tables_list: {e}")
            raise
        finally:
            # asyncpg handles connection release automatically
            pass

    async def drop_all_tables(self):
        """
        Drop all tables in the database.

        Logs detailed messages about each step and continues attempting to delete remaining tables
        even if one fails due to some reason (like foreign key constraints etc.)
        """
        tables = await self.get_tables_list()
        if tables:
            logger.debug(f"{self.__class__.__name__}: Starting mass deletion of all tables...")
            # Try to drop each table individually but log any errors
            for table in tables:
                await self.drop_table(table)
            logger.debug(f"{self.__class__.__name__}: Mass deletion process completed")
        else:
            logger.debug(f"{self.__class__.__name__}: List of tables empty")


# Example usage:
async def main():
    pool = await create_pool(host=ConfigPostgreSQL.HOST,
                             database=ConfigPostgreSQL.DATABASE,
                             user=ConfigPostgreSQL.USER,
                             password=ConfigPostgreSQL.PASSWORD)
    asql = AsyncSQLMeta(pool)
    exists = await asql.is_table_exists('models_cards_base')
    print(exists)
    # print(asql.idnum)
    # asql_1 = AsyncSQLMeta(pool)
    # print(asql_1.idnum)


if __name__ == "__main__":
    asyncio.run(main())

# test_sql = SQLMeta(host=ConfigPostgreSQL.HOST,
#                    database=ConfigPostgreSQL.DATABASE,
#                    user=ConfigPostgreSQL.USER,
#                    password=ConfigPostgreSQL.PASSWORD,
#                    )
# base_exists = test_sql.is_table_exists('models_cards_base')
# print(base_exists)
# del test_sql
# test_sql = SQLMeta(host=ConfigPostgreSQL.HOST,
#                    database=ConfigPostgreSQL.DATABASE,
#                    user=ConfigPostgreSQL.USER,
#                    password=ConfigPostgreSQL.PASSWORD,
#                    )
# print('idnum #', test_sql.idnum)
