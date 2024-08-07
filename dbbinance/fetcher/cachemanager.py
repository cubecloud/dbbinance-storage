import sys
from typing import Union
from collections import OrderedDict
from mlthread_tools import mlt_mutex
from mlthread_tools import mlp_mutex

import logging

__version__ = 0.015

logger = logging.getLogger()


class CacheManager:
    def __init__(self, max_memory_gb: Union[float, int] = 3, mutex: str = 'mlt'):
        """
        Initialize the Cache class with an optional maximum memory limit in gigabytes.

        Parameters:
            max_memory_gb (float or int): The maximum memory limit in gigabytes.

        Returns:
            None
        """

        self.__cache = OrderedDict()
        self.max_memory_bytes = int(max_memory_gb * 1024 * 1024 * 1024)  # Convert max_memory_gb to bytes
        self.current_memory_usage = 0
        if mutex == 'mlt':
            self.lock = mlt_mutex
        elif mutex == 'mp':
            self.lock = mlp_mutex
        else:
            sys.exit(f'Error: Unknown option {mutex}')

    @property
    def cache(self):
        return self.__cache

    def update_cache(self, key, value):
        with self.lock:
            value_size = sys.getsizeof(value)
            if value_size > self.max_memory_bytes:
                logger.warning(f"{self.__class__.__name__}: "
                               f"Object size is greater then {self.max_memory_bytes} increase CacheManager memory")
            while (self.current_memory_usage + value_size > self.max_memory_bytes) and len(self.__cache) > 0:
                # Delete the oldest item to free up memory
                self.__cache.popitem(last=False)
            self.__cache.update({key: value})
            self.current_memory_usage = sum(sys.getsizeof(v) for v in self.__cache.values())

    def popitem(self, last=False):
        with self.lock:
            item = self.__cache.popitem(last=last)
            self.current_memory_usage = sum(sys.getsizeof(v) for v in self.__cache.values())
        return item

    @staticmethod
    def get_cache_key(**cache_kwargs):
        return tuple(sorted(cache_kwargs.items()))
