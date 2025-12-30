import logging
import datetime
import pandas as pd
from datetime import timezone

from dbbinance.fetcher import Constants, DataRepair, DataFetcher, DataUpdater
from dbbinance.config.configpostgresql import ConfigPostgreSQL
from dbbinance.config.configbinance import ConfigBinance

logger = logging.getLogger()

# Set up logging handlers
logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler('datafetcher.log')
file_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

logging.getLogger('apscheduler').setLevel(logging.DEBUG)

""" Initialize DataFetcher """
fetcher = DataUpdater(host=ConfigPostgreSQL.HOST,
                      database=ConfigPostgreSQL.DATABASE,
                      user=ConfigPostgreSQL.USER,
                      password=ConfigPostgreSQL.PASSWORD,
                      binance_api_key=ConfigBinance.BINANCE_API_KEY,
                      binance_api_secret=ConfigBinance.BINANCE_API_SECRET,
                      )


def get_data_without_resampling(table_name, start=None, end=None, use_cols=None, use_dtypes=None):
    """
    Fetch data without resampling
    """
    if not start:
        start = datetime.datetime.strptime('01 Aug 2017', '%d %b %Y').replace(tzinfo=timezone.utc)
    if not end:
        end = datetime.datetime.now(timezone.utc)

    data_df = fetcher.get_data_as_df(table_name=table_name,
                                     start=start,
                                     end=end,
                                     use_cols=use_cols,
                                     use_dtypes=use_dtypes,
                                     )  # Disable resampling

    data_df['open_time'] = pd.to_datetime(data_df['open_time'], unit='ms')
    data_df['open_time'] = DataRepair.convert_timestamp_to_datetime(data_df['open_time'])
    data_df = data_df.set_index('open_time', inplace=False)

    return data_df


def _fetch_data(queue, identifier):
    """
    Helper function for multithreaded data fetching
    """
    try:
        data = get_data_without_resampling(
            table_name="spot_data_btcusdt_1m",
            start=datetime.datetime.strptime('01 Aug 2017', '%d %b %Y').replace(tzinfo=timezone.utc),
            end=datetime.datetime.now(timezone.utc),
            use_cols=Constants.ohlcv_cols,
            use_dtypes=Constants.ohlcv_dtypes
        )
        queue.put((identifier, data))
    except Exception as e:
        queue.put((identifier, e))


def test_multithreaded_fetch(num_threads=5):
    """
    Test fetching data from multiple threads and compare results
    """
    # Get baseline data
    baseline_data = get_data_without_resampling(
        table_name="spot_data_btcusdt_1m",
        start=datetime.datetime.strptime('01 Aug 2017', '%d %b %Y').replace(tzinfo=timezone.utc),
        end=datetime.datetime.now(timezone.utc),
        use_cols=Constants.ohlcv_cols,
        use_dtypes=Constants.ohlcv_dtypes
    )

    # Create queue and threads
    from threading import Thread
    from queue import Queue

    q = Queue()
    threads = []

    for i in range(num_threads):
        t = Thread(target=_fetch_data, args=(q, i))
        threads.append(t)
        t.start()

    # Collect results
    results = []
    exceptions = []

    while len(results) < num_threads:
        identifier, result_or_exception = q.get()
        if isinstance(result_or_exception, Exception):
            exceptions.append((identifier, result_or_exception))
        else:
            results.append((identifier, result_or_exception))

    # Check all threads finished
    for t in threads:
        t.join()

    # Verify results
    if exceptions:
        logger.error(f"Exceptions occurred in {len(exceptions)} threads:")
        for ident, exc in exceptions:
            logger.error(f"Thread {ident}: {str(exc)}")
        return False

    # Compare data from all threads with baseline
    for ident, data in results:
        if not pd.DataFrame.equals(data, baseline_data):
            logger.error(f"Data mismatch detected in thread {ident}")
            return False

    logger.info("All threads returned identical data")
    return True


# Run tests
if __name__ == '__main__':
    # Test single-threaded data fetch
    print("\nTesting single-threaded data fetch...\n")
    baseline_data = get_data_without_resampling(
        table_name="spot_data_btcusdt_1m",
        start=datetime.datetime.strptime('01 Aug 2017', '%d %b %Y').replace(tzinfo=timezone.utc),
        end=datetime.datetime.now(timezone.utc),
        use_cols=Constants.ohlcv_cols,
        use_dtypes=Constants.ohlcv_dtypes
    )

    print("\nBaseline data head:")
    print(baseline_data.head(5).to_string())

    # Test multithreaded fetch and comparison
    print("\nTesting multithreaded data fetch...\n")
    success = test_multithreaded_fetch(num_threads=5)
    if success:
        print("\nAll threads successfully retrieved identical data!\n")
    else:
        print("\nData inconsistency detected\n")
