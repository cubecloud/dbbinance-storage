import sys
from typing import Union
from collections import OrderedDict
from mlthread_tools import mlt_mutex
from mlthread_tools import mlp_mutex

import logging

__version__ = 0.020

logger = logging.getLogger()


class CacheDict(OrderedDict):
    def __init__(self, *args, **kwargs):
        self.hits = {}
        super(CacheDict, self).__init__(*args, **kwargs)

    def __getitem__(self, key):
        if key not in self.hits:
            self.hits[key] = 0
        self.hits[key] += 1
        return super(CacheDict, self).__getitem__(key)

    def __delitem__(self, key, dict_delitem=dict.__delitem__):
        super(CacheDict, self).__delitem__(key)
        del self.hits[key]

    def __setitem__(self, key, value):
        super(CacheDict, self).__setitem__(key, value)
        self.hits[key] = 0

    def get(self, key, default=None):
        value = super(CacheDict, self).get(key, default)
        if key in self.hits:
            self.hits[key] += 1
        return value

    def clear(self):
        super(CacheDict, self).clear()
        self.hits = {}

    def pop(self, key):
        value = super(CacheDict, self).pop(key)
        if key in self.hits:
            del self.hits[key]
        return value

    def popitem(self, last=True):
        key, value = super(CacheDict, self).popitem(last)
        if key in self.hits:
            del self.hits[key]
        return key, value

    def items(self):
        _odict = super(CacheDict, self).items()
        for key, _ in _odict:
            self.hits[key] += 1
        return _odict

    def values(self):
        for key in super(CacheDict, self).keys():
            self.hits[key] += 1
        return super(CacheDict, self).values()

    def keys_probs(self) -> dict:
        total = sum(self.hits.values())
        return {k: v / total for k, v in self.hits.items()}


class CacheManager:
    def __init__(self, max_memory_gb: Union[float, int] = 3, mutex: str = 'mlt'):
        """
        Initialize the Cache class with an optional maximum memory limit in gigabytes.

        Parameters:
            max_memory_gb (float or int): The maximum memory limit in gigabytes.

        Returns:
            None
        """

        if mutex == 'mlt':
            self.lock = mlt_mutex
        elif mutex == 'mlp':
            self.lock = mlp_mutex
        else:
            sys.exit(f'Error: Unknown option {mutex}')
        self.mutex_type = mutex
        self.__cache = CacheDict()
        self.max_memory_bytes = int(max_memory_gb * 1024 * 1024 * 1024)  # Convert max_memory_gb to bytes
        self.current_memory_usage = self.__cache.__sizeof__()

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
            self.current_memory_usage = self.__cache.__sizeof__()

    def popitem(self, last=False):
        with self.lock:
            item = self.__cache.popitem(last=last)
            self.current_memory_usage = self.__cache.__sizeof__()
        return item

    def pop(self, key):
        with self.lock:
            item = self.__cache.pop(key)
            self.current_memory_usage = self.__cache.__sizeof__()
        return item

    @staticmethod
    def get_cache_key(**cache_kwargs):
        return tuple(sorted(cache_kwargs.items()))
