from typing import Union
from collections import OrderedDict
from mlthread_tools import mlt_mutex
import objsize
import logging

__version__ = 0.030

logger = logging.getLogger()


class FetcherCacheManager:
    def __init__(self,
                 max_memory_gb: Union[float, int] = 3,
                 mlt_rlock=None):
        """
        Initialize the Cache class with an optional maximum memory limit in gigabytes.
        CacheManager using mlt_mutex from mlthread_tools package

        Args:
            max_memory_gb (float or int):   The maximum memory limit in gigabytes
        """
        if mlt_rlock is None:
            self.lock = mlt_mutex
        else:
            self.lock = mlt_rlock

        self.__cache = OrderedDict()
        self.max_memory_bytes = int(max_memory_gb * 1024 * 1024 * 1024)  # Convert max_memory_gb to bytes
        self.current_memory_usage = self.cache_size()

    @property
    def cache(self):
        return self.__cache

    def update(self, key_value_dict: dict):
        for key, value in key_value_dict.items():
            self.update_cache(key, value)

    def update_cache(self, key, value):
        with self.lock:
            value_size = objsize.get_deep_size(value) + objsize.get_deep_size(key)
            if value_size > self.max_memory_bytes:
                logger.warning(f"{self.__class__.__name__}: "
                               f"Object size is greater then {self.max_memory_bytes} increase CacheManager memory")
            while (self.current_memory_usage + value_size > self.max_memory_bytes) and (self.__len__() > 0):
                # Delete the oldest item to free up memory
                self.__cache.popitem(last=False)
            self.__cache.update({key: value})
            self.current_memory_usage += (value_size + objsize.get_deep_size(key) + objsize.get_deep_size(1))

    def popitem(self, last=False):
        with self.lock:
            item = self.__cache.popitem(last=last)
            self.current_memory_usage = self.cache_size()
        return item

    def pop(self, key):
        with self.lock:
            item = self.__cache.pop(key)
            self.current_memory_usage = self.cache_size()
        return item

    def get(self, key, default=None):
        with self.lock:
            return self.__cache.get(key, default)

    def clear(self):
        with self.lock:
            self.__cache.clear()

    def keys(self):
        with self.lock:
            return self.__cache.keys()

    def items(self):
        with self.lock:
            return self.__cache.items()

    def values(self):
        with self.lock:
            return self.__cache.values()

    def __len__(self):
        with self.lock:
            return self.__cache.__len__()

    @staticmethod
    def get_cache_key(**cache_kwargs):
        return tuple(sorted(cache_kwargs.items()))

    def update_cache_size(self) -> None:
        self.current_memory_usage = self.cache_size()

    def cache_size(self):
        with self.lock:
            size = objsize.get_deep_size(self.__cache)  # instance dictionary
            for k, v in self.__cache.items():
                size += objsize.get_deep_size(k)
                size += objsize.get_deep_size(v)
        return size


