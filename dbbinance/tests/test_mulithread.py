import threading
import time
import random
from dbbinance.fetcher.sqlbase import DBConnectionManager
import logging
from psycopg2 import sql

# Configure logging for debugging
logging.basicConfig(level=logging.DEBUG)


def test_insert_data(thread_id, db_manager):
    try:
        print(f"Thread {thread_id}: Starting insert")
        query = sql.SQL("""
            INSERT INTO test_table (value)
            VALUES (%s);
        """)

        # Generate random data
        value = random.randint(1, 100)

        with db_manager as conn:
            cur = conn.cursor()
            cur.execute(query, (value,))
            conn.commit()

        print(f"Thread {thread_id}: Inserted value {value}")
    except Exception as e:
        print(f"Thread {thread_id}: Error in insert - {e}")


def test_update_data(thread_id, db_manager):
    try:
        print(f"Thread {thread_id}: Starting update")
        query = sql.SQL("""
            UPDATE test_table
            SET value = value + 1
            WHERE id = %s;
        """)

        # Select random row to update
        select_query = sql.SQL("SELECT id FROM test_table LIMIT 1;")

        with db_manager as conn:
            cur = conn.cursor()

            # Get a random ID
            cur.execute(select_query)
            result = cur.fetchone()
            if result:
                row_id = result[0]

                # Update the row
                cur.execute(query, (row_id,))
                conn.commit()

                print(f"Thread {thread_id}: Updated row {row_id}")
    except Exception as e:
        print(f"Thread {thread_id}: Error in update - {e}")


def test_select_all(thread_id, db_manager):
    try:
        print(f"Thread {thread_id}: Starting select")
        query = sql.SQL("SELECT * FROM test_table;")

        with db_manager as conn:
            cur = conn.cursor()
            cur.execute(query)
            results = cur.fetchall()

            print(f"Thread {thread_id}: Retrieved {len(results)} rows")
    except Exception as e:
        print(f"Thread {thread_id}: Error in select - {e}")


if __name__ == "__main__":

    from dbbinance.config.configpostgresql import ConfigPostgreSQL
    from dbbinance.fetcher.sqlbase import SQLMeta

    # Initialize the DB connection manager
    db_manager = DBConnectionManager(
        host=ConfigPostgreSQL.HOST,
        database=ConfigPostgreSQL.DATABASE,
        user=ConfigPostgreSQL.USER,
        password=ConfigPostgreSQL.PASSWORD,
        minconn=1,
        maxconn=10
    )

    # Create test table if not exists
    create_table_query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS test_table (
            id SERIAL PRIMARY KEY,
            value INTEGER
        );
    """)

    db_manager.modify_query(create_table_query)

    # Configuration
    num_threads = 3  # Number of threads per operation type
    delay = 0.5  # Delay between operations (seconds)
    num_runs = 2  # Number of test runs

    for run in range(num_runs):
        print(f"\nRun #{run + 1} starting...")

        # Create thread pool
        threads = []

        # Start insert threads
        for i in range(num_threads):
            t = threading.Thread(target=test_insert_data, args=(i + 1, db_manager))
            threads.append(t)

        # Start update threads
        for i in range(num_threads):
            t = threading.Thread(target=test_update_data, args=(num_threads + i + 1, db_manager))
            threads.append(t)

        # Start select threads
        for i in range(num_threads):
            t = threading.Thread(target=test_select_all, args=(2 * num_threads + i + 1, db_manager))
            threads.append(t)

        # Start all threads
        for thread in threads:
            time.sleep(delay)  # Add delay between thread starts
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        print(f"\nRun #{run + 1} completed successfully")

    print("\nAll tests completed")
