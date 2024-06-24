import logging
import psycopg2
from collections import OrderedDict


__version__ = 0.007

logger = logging.getLogger()


def handle_errors(func):
    def wrapper(self, *args, **kwargs):
        try:
            result = func(self, *args, **kwargs)
            return result
        except psycopg2.Error as e:
            logger.error(f"Error: {e}")

    return wrapper


class SQLMeta:
    count = 0

    def __init__(self, host, database, user, password, max_conn=10):
        SQLMeta.count += 1
        self.idnum = int(SQLMeta.count)
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.max_memorized_conn = max_conn
        self.__connections = OrderedDict()
        """ Get external data with logger """
        logger.debug(f"{self.__class__.__name__} #{self.idnum}': Initializing...")

    @property
    def conn(self):
        conn = self.get_conn()
        return conn

    def get_conn(self):
        """ Connect to database """
        conn = psycopg2.connect(host=self.host,
                                database=self.database,
                                user=self.user,
                                password=self.password
                                )
        return conn

    def is_table_exists(self, table_name: str):
        with self.conn.cursor() as cur:
            cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s)", (table_name,))
            result = cur.fetchone()
        return result[0]

    @handle_errors
    def drop_table(self, table_name):
        conn = self.conn  # Used for optimizing calls of @property
        with conn.cursor() as cur:
            # Start a transaction
            cur.execute("BEGIN;")
            cur.execute(f"LOCK TABLE {table_name} IN ACCESS EXCLUSIVE MODE;")
            cur.execute(f"DROP TABLE {table_name}")
        conn.commit()
        conn.close()
        logger.debug(f"{self.__class__.__name__}: DROP TABLE '{table_name}'")

    @handle_errors
    def drop_all_tables(self):
        tables = self.get_tables_list()
        msg = f"DROPPING ALL TABLES"
        logger.debug(msg)
        for table in tables:
            self.drop_table(table[0])

    @handle_errors
    def get_tables_list(self):
        with self.conn.cursor() as cur:
            query = "SELECT table_name " \
                    "FROM information_schema.tables " \
                    "WHERE table_type = 'BASE TABLE' AND table_schema = 'public'"
            cur.execute(query)
            tables = cur.fetchall()
        return tables

    def __del__(self):
        SQLMeta.count -= 1


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

