import logging
import datetime
import pandas as pd
from datetime import timezone
from threading import Thread
from queue import Queue

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
fetcher = DataFetcher(host=ConfigPostgreSQL.HOST,
                      database=ConfigPostgreSQL.DATABASE,
                      user=ConfigPostgreSQL.USER,
                      password=ConfigPostgreSQL.PASSWORD,
                      binance_api_key="dummy",
                      binance_api_secret="dummy",
                      )

updater = DataUpdater(host=ConfigPostgreSQL.HOST,
                      database=ConfigPostgreSQL.DATABASE,
                      user=ConfigPostgreSQL.USER,
                      password=ConfigPostgreSQL.PASSWORD,
                      binance_api_key="dummy",
                      binance_api_secret="dummy",
                      )


def get_data_without_resampling(table_name, start=None, end=None, use_cols=None, use_dtypes=None):
    """
    Fetch data without resampling
    """
    if not start:
        start = datetime.datetime.strptime('01 Aug 2017', '%d %b %Y').replace(tzinfo=timezone.utc)
    if not end:
        end = datetime.datetime.now(timezone.utc)

    data_df = updater.get_data_as_df(table_name=table_name,
                                     start=start,
                                     end=end,
                                     use_cols=use_cols,
                                     use_dtypes=use_dtypes,
                                     )  # Disable resampling

    data_df['open_time'] = pd.to_datetime(data_df['open_time'], unit='ms')
    data_df['open_time'] = DataRepair.convert_timestamp_to_datetime(data_df['open_time'])
    data_df = data_df.set_index('open_time', inplace=False)

    return data_df


def get_resampled_data(table_name, timeframe, start=None, end=None, use_cols=None, use_dtypes=None, cached=False):
    """
    Fetch resampled data
    """
    if not start:
        start = datetime.datetime.strptime('01 Aug 2017', '%d %b %Y').replace(tzinfo=timezone.utc)
    if not end:
        end = datetime.datetime.now(timezone.utc)

    data_df = fetcher.resample_to_timeframe(
        table_name=table_name,
        to_timeframe=timeframe,
        start=start,
        end=end,
        use_cols=use_cols,
        use_dtypes=use_dtypes,
        cached=cached
    )

    return data_df


def _fetch_data_without_resampling(queue, identifier):
    """
    Helper function for multithreaded data fetching (without resampling)
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


def _fetch_resampled_data(queue, identifier):
    """
    Helper function for multithreaded resampled data fetching
    """
    try:
        data = get_resampled_data(
            table_name="spot_data_btcusdt_1m",
            timeframe="1h",
            start=datetime.datetime.strptime('01 Aug 2017', '%d %b %Y').replace(tzinfo=timezone.utc),
            end=datetime.datetime.now(timezone.utc),
            use_cols=Constants.ohlcv_cols,
            use_dtypes=Constants.ohlcv_dtypes
        )
        queue.put((identifier, data))
    except Exception as e:
        queue.put((identifier, e))


def test_multithreaded_fetch_without_resampling(num_threads=5):
    """
    Test fetching data without resampling from multiple threads and compare results
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
    q = Queue()
    threads = []

    for i in range(num_threads):
        t = Thread(target=_fetch_data_without_resampling, args=(q, i))
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

    logger.info("All threads returned identical data (without resampling)")
    return True


def test_multithreaded_fetch_resampled(num_threads=5):
    """
    Test fetching resampled data from multiple threads and compare results
    """
    # Get baseline data
    baseline_data = get_resampled_data(
        table_name="spot_data_btcusdt_1m",
        timeframe="1h",
        start=datetime.datetime.strptime('01 Aug 2017', '%d %b %Y').replace(tzinfo=timezone.utc),
        end=datetime.datetime.now(timezone.utc),
        use_cols=Constants.ohlcv_cols,
        use_dtypes=Constants.ohlcv_dtypes,
        cached=True
    )

    # Create queue and threads
    q = Queue()
    threads = []

    for i in range(num_threads):
        t = Thread(target=_fetch_resampled_data, args=(q, i))
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

    logger.info("All threads returned identical resampled data")
    return True


def test_resampling_consistency():
    """
    Test that resampling produces consistent results across different threads
    """
    logger.info("Starting resampling consistency test...")

    # Get baseline data
    baseline_1h = get_resampled_data(
        table_name="spot_data_btcusdt_1m",
        timeframe="1h",
        start=datetime.datetime.strptime('01 Aug 2017', '%d %b %Y').replace(tzinfo=timezone.utc),
        end=datetime.datetime.now(timezone.utc),
        use_cols=Constants.ohlcv_cols,
        use_dtypes=Constants.ohlcv_dtypes,
        cached=True
    )

    baseline_5m = get_resampled_data(
        table_name="spot_data_btcusdt_1m",
        timeframe="5m",
        start=datetime.datetime.strptime('01 Aug 2017', '%d %b %Y').replace(tzinfo=timezone.utc),
        end=datetime.datetime.now(timezone.utc),
        use_cols=Constants.ohlcv_cols,
        use_dtypes=Constants.ohlcv_dtypes,
        cached=True
    )

    logger.info(f"Baseline 1h data shape: {baseline_1h.shape}")
    logger.info(f"Baseline 5m data shape: {baseline_5m.shape}")

    # Test consistency with multiple threads
    success_1h = test_multithreaded_fetch_resampled(num_threads=3)
    success_5m = test_multithreaded_fetch_resampled(num_threads=3)

    if success_1h and success_5m:
        logger.info("Resampling consistency test PASSED")
        return True
    else:
        logger.error("Resampling consistency test FAILED")
        return False


# Run tests
if __name__ == '__main__':
    # Test single-threaded data fetch (without resampling)
    print("\nTesting single-threaded data fetch (without resampling)...\n")
    baseline_data = get_data_without_resampling(
        table_name="spot_data_btcusdt_1m",
        start=datetime.datetime.strptime('01 Aug 2017', '%d %b %Y').replace(tzinfo=timezone.utc),
        end=datetime.datetime.now(timezone.utc),
        use_cols=Constants.ohlcv_cols,
        use_dtypes=Constants.ohlcv_dtypes
    )

    print("\nBaseline data head:")
    print(baseline_data.head(5).to_string())

    # Test multithreaded fetch and comparison (without resampling)
    print("\nTesting multithreaded data fetch (without resampling)...\n")
    success = test_multithreaded_fetch_without_resampling(num_threads=5)
    if success:
        print("\nAll threads successfully retrieved identical data (without resampling)!\n")
    else:
        print("\nData inconsistency detected (without resampling)\n")

    # Test single-threaded resampling
    print("\nTesting single-threaded resampling...\n")
    resampled_data = get_resampled_data(
        table_name="spot_data_btcusdt_1m",
        timeframe="1h",
        start=datetime.datetime.strptime('01 Aug 2017', '%d %b %Y').replace(tzinfo=timezone.utc),
        end=datetime.datetime.now(timezone.utc),
        use_cols=Constants.ohlcv_cols,
        use_dtypes=Constants.ohlcv_dtypes
    )

    print("\nResampled data head:")
    print(resampled_data.head(5).to_string())

    # Test multithreaded resampling
    print("\nTesting multithreaded resampling...\n")
    success = test_multithreaded_fetch_resampled(num_threads=5)
    if success:
        print("\nAll threads successfully retrieved identical resampled data!\n")
    else:
        print("\nData inconsistency detected in resampled data\n")

    # Run comprehensive consistency test
    print("\nRunning comprehensive resampling consistency test...\n")
    consistency_success = test_resampling_consistency()
    if consistency_success:
        print("\nAll resampling consistency tests PASSED!\n")
    else:
        print("\nResampling consistency tests FAILED!\n")
