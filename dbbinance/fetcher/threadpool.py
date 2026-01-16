import logging
import psycopg2
from typing import Optional, Any
from psycopg2.pool import ThreadedConnectionPool

_pool: Optional[ThreadedConnectionPool] = None


def create_pool(database=None, user=None, password=None, host='localhost', port=5432, minconn=1,
                maxconn=12):
    pool = ThreadedConnectionPool(
        dbname=database,
        user=user,
        password=password,
        minconn=minconn,
        maxconn=maxconn,
        host=host,
        port=port
    )
    return pool


class ThreadPool:
    """Class managing database connections through a connection pool."""

    def __init__(self, pool: Optional[ThreadedConnectionPool] = None):
        # Create a new connection pool instance
        if pool is not None:
            self.pool = pool
        else:
            raise 'Error: Pool not provided. Please provide a valid ThreadPool'
        self.conn = None

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
