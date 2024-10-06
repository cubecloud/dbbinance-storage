import sys
import objsize
import time
import datetime

from mlthread_tools.multiprocessing_mutex import mlp_mutex
from multiprocessing import Process, Manager, Value
import pandas as pd

from dbbinance.fetcher import MpCacheManager
import multiprocessing


def update_cache(manager, process_id):
    for i in range(10):
        key = f"key_{process_id}_{i}"
        value = f"value_{process_id}_{i}"
        manager.update({key: value})
        print(f"Process {process_id} updated cache: {key} = {value}")


def run_client():
    manager = MpCacheManager(port=5500, start_host=False)
    processes = []
    for i in range(multiprocessing.cpu_count() - 1):
        p = multiprocessing.Process(target=update_cache, args=(manager, i))
        processes.append(p)
        p.start()
    for p in processes:
        p.join()
    print("Final cache contents:")
    for key, value in manager.items():
        print(f"{key} = {value}")


if __name__ == '__main__':
    run_client()
