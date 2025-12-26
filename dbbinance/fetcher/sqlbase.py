import logging
import psycopg2
from typing import List, Tuple, Optional
from psycopg2 import sql
from psycopg2.pool import SimpleConnectionPool, ThreadedConnectionPool
from collections import OrderedDict

__version__ = 0.013

logger = logging.getLogger()


def handle_errors(func):
    def wrapper(self, *args, **kwargs):
        try:
            result = func(self, *args, **kwargs)
            return result
        except psycopg2.Error as e:
            logger.error(f"Error: {e}")

    return wrapper


class DBConnectionManager:
    """Class managing database connections through a connection pool."""

    def __init__(self, database=None, user=None, password=None, minconn=1, maxconn=10, host='localhost'):
        """
        Initializes the connection pool with provided configuration settings.

        Args:
            database (str): Name of the database to connect to.
            user (str): User account used for authentication.
            password (str): Password associated with the user account.
            minconn (int): Minimum number of connections maintained in the pool.
            maxconn (int): Maximum number of connections allowed in the pool.
            host (str): Address of the database server (default is localhost).
        """
        # Create a new connection pool instance
        self.pool = ThreadedConnectionPool(
            dbname=database,
            user=user,
            password=password,
            minconn=minconn,
            maxconn=maxconn,
            host=host
        )

    def __enter__(self):
        """
        Enter the runtime context related to this object.
        Acquires a connection from the pool when entering a `with` block.

        Returns:
          A connection object retrieved from the pool.
        """
        self.conn = self.pool.getconn()
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the runtime context related to this object.
        Releases the connection back into the pool when leaving a `with` block.
        Propagates any unhandled exceptions up the call stack.

        Args:
          exc_type (type): Type of the exception raised (or None if no exception was raised).
          exc_val (Exception): Value of the exception raised (or None if no exception was raised).
          exc_tb (traceback): Traceback information about the exception (or None if no exception was raised).
        """
        if self.conn is not None:
            self.pool.putconn(self.conn)

        # Re-raise any exception that may have been caught inside the context block
        if exc_type is not None:
            raise exc_val.with_traceback(exc_tb)

    def __del__(self):
        """
        Destructor, cleans up resources like closing the connection pool.
        """
        if hasattr(self, 'pool') and self.pool is not None:
            self.pool.closeall()

    def modify_query(self, query, params=None) -> bool:
        """
        Execute a modifying query (CREATE, INSERT, UPDATE, DELETE) without fetching results.

        Args:
            query (sql.SQL | str): SQL query to execute.
            params (tuple|list, optional): Parameters to substitute into the query.
        """
        try:
            with self.__enter__():
                with self.conn.cursor() as cur:
                    cur.execute(query, params)
                    self.conn.commit()
            return True
        except psycopg2.pool.PoolError as e:
            print(f"Error getting connection from pool - {e}")
        except psycopg2.Error as e:
            logger.debug(f"Database error: {e}")
            raise
        except Exception as ex:
            logger.debug(f"Unexpected error: {ex}")
            raise
        finally:
            # Ensure the cursor is always closed regardless of success/failure
            if hasattr(self, 'conn') and getattr(self, 'conn', None) is not None:
                cur.close()

    def single_select_query(self, query, params=None) -> Optional[Tuple]:
        """
        Execute a SELECT query expecting exactly one row and return it.

        Args:
            query (sql.SQL | str): SQL query to execute.
            params (tuple|list, optional): Parameters to substitute into the query.

        Returns:
            tuple or None: Single row returned by the query execution.
        """

        try:
            with self.__enter__():
                with self.conn.cursor() as cur:
                    cur.execute(query, params)
                    result = cur.fetchone()
                    return result
        except psycopg2.pool.PoolError as e:
            print(f"Error getting connection from pool - {e}")
        except psycopg2.Error as e:
            logger.debug(f"Database error: {e}")
            raise
        except Exception as ex:
            logger.debug(f"Unexpected error: {ex}")
            raise
        finally:
            # Ensure the cursor is always closed regardless of success/failure
            if hasattr(self, 'conn') and getattr(self, 'conn', None) is not None:
                cur.close()

    def select_query(self, query, params=None) -> List[Tuple]:
        """
        Execute a SELECT query and return its result.

        Args:
            query (sql.SQL | str): SQL query to execute.
            params (tuple|list, optional): Parameters to substitute into the query.

        Returns:
            list[tuple]: Result set returned by the query execution.
        """
        try:
            with self.__enter__():
                with self.conn.cursor() as cur:
                    cur.execute(query, params)
                    result = cur.fetchall()
                    return result
        except psycopg2.pool.PoolError as e:
            print(f"Error getting connection from pool - {e}")
        except psycopg2.Error as e:
            logger.debug(f"Database error: {e}")
            raise
        except Exception as ex:
            logger.debug(f"Unexpected error: {ex}")
            raise
        finally:
            # Ensure the cursor is always closed regardless of success/failure
            if hasattr(self, 'conn') and getattr(self, 'conn', None) is not None:
                cur.close()


class SQLMeta:
    count = 0

    def __init__(self, host, database, user, password, minconn=1, maxconn=10):
        SQLMeta.count += 1
        self.idnum = int(SQLMeta.count)
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.max_memorized_conn = maxconn
        self.db_mgr = DBConnectionManager(host=host,
                                          database=database,
                                          user=user,
                                          password=password,
                                          minconn=1,
                                          maxconn=maxconn)

        self.__connections = OrderedDict()  # Will be Deprecated in future

        """ Get external data with logger """
        logger.debug(f"{self.__class__.__name__} #{self.idnum}': Initializing...")

    def __del__(self):
        SQLMeta.count -= 1
        del self.db_mgr

    def is_table_exists(self, table_name: str) -> bool:
        """
        Check the table existence in database

        Args:
            table_name (str):   table name

        Returns:
            bool: True, if table exist, False, if NOT exist.
        """
        with self.db_mgr as conn:
            with conn.cursor() as cur:
                # Use sql.Literal instead of sql.Identifier since we're passing a literal value
                query = sql.SQL("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = {}
                    );
                """).format(sql.Literal(table_name))  # Use Literal for strings

                # Execute the prepared query
                cur.execute(query)
                result = cur.fetchone()[0]
                logger.debug(f"{self.__class__.__name__}: Checked existence of table '{table_name}': {result}")
        return bool(result)

    @handle_errors
    def drop_table(self, table_name) -> bool:
        """
        Drop a table from the database with exclusive lock.

        Args:
            table_name (str): Name of the table to drop.

        Returns:
            bool: True if the operation succeeded, False otherwise.
        """
        try:
            with self.db_mgr as conn:
                with conn.cursor() as cur:
                    # Exclusive lock the table before dropping
                    cur.execute(sql.SQL("LOCK TABLE {} IN ACCESS EXCLUSIVE MODE;").format(sql.Literal(table_name)))

                    # Safely drop the table
                    cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(sql.Literal(table_name)))

                # Explicitly commit the changes
                conn.commit()

            logger.debug(f"{self.__class__.__name__}: DROP TABLE '{table_name}'")
            return True  # Operation successful
        except Exception as e:
            logger.error(f"{self.__class__.__name__}: Failed to drop table '{table_name}': {e}")
            return False  # Operation failed

    @handle_errors
    def get_tables_list(self) -> List:
        """
        Retrieve a list of public schema's base tables from the database.

        Returns:
            list: List of table names.
        """

        with self.db_mgr as conn:
            with conn.cursor() as cur:
                query = sql.SQL("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_type = 'BASE TABLE' 
                      AND table_schema = {}
                """).format(sql.Literal('public'))

                cur.execute(query)
                tables = [row[0] for row in cur.fetchall()]
                logger.debug(f"{self.__class__.__name__}: Retrieved list of tables: {tables}")
        return tables

    @handle_errors
    def drop_all_tables(self):
        """
        Drop all tables in the database.

        Logs detailed messages about each step and continues attempting to delete remaining tables
        even if one fails due to some reason (like foreign key constraints etc.)
        """
        tables = self.get_tables_list()
        if tables:
            logger.debug(f"{self.__class__.__name__}: Starting mass deletion of all tables...")
            # Try to drop each table individually but log any errors
            for table in tables:
                self.drop_table(table)
            logger.debug(f"{self.__class__.__name__}: Mass deletion process completed")
        else:
            logger.debug(f"{self.__class__.__name__}: List of tables empty")


if __name__ == "__main__":
    from dbbinance.config.configpostgresql import ConfigPostgreSQL

    test_sql = SQLMeta(host=ConfigPostgreSQL.HOST,
                       database=ConfigPostgreSQL.DATABASE,
                       user=ConfigPostgreSQL.USER,
                       password=ConfigPostgreSQL.PASSWORD,
                       )
    base_exists = test_sql.is_table_exists('models_cards_base')
    print(base_exists)
    del test_sql
    test_sql = SQLMeta(host=ConfigPostgreSQL.HOST,
                       database=ConfigPostgreSQL.DATABASE,
                       user=ConfigPostgreSQL.USER,
                       password=ConfigPostgreSQL.PASSWORD,
                       )
    print('idnum #', test_sql.idnum)
