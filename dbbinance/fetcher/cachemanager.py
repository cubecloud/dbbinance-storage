import sys
from typing import Union
from collections import OrderedDict
from mlthread_tools import mlt_mutex
import objsize
import logging
import multiprocessing as mp

__version__ = 0.027

logger = mp.get_logger()
# logger = logging.getLogger()


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

    def hits_probs(self) -> dict:
        total = sum(self.hits.values())
        _p = {k: v / total for k, v in self.hits.items()}
        return dict(sorted(_p.items(), key=lambda x: x[1], reverse=False))


class CacheManager:
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

        self.__cache = CacheDict()
        self.__hits: dict = {}
        self.max_memory_bytes = int(max_memory_gb * 1024 * 1024 * 1024)  # Convert max_memory_gb to bytes
        self.current_memory_usage = self.cache_size()

    @property
    def cache(self):
        return self.__cache

    @property
    def hits(self):
        return self.__hits
    
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
            self.__hits.update({key: 1})
            self.current_memory_usage += (value_size + objsize.get_deep_size(key) + objsize.get_deep_size(1))

    def popitem(self, last=False):
        with self.lock:
            if last:
                idx = -1
            else:
                idx = 0
            key = list(self.__cache.keys())[idx]
            del self.__hits[key]
            item = self.__cache.pop(key)
            self.current_memory_usage = self.cache_size()
        return item

    def pop(self, key):
        with self.lock:
            del self.__hits[key]
            item = self.__cache.pop(key)
            self.current_memory_usage = self.cache_size()
        return item

    def get(self, key, default=None):
        value = self.__cache.get(key, None)
        if value is not None:
            with self.lock:
                if key not in self.__hits:
                    self.__hits[key] = 0
                self.__hits[key] += 1
        else:
            value = default
        return value

    def clear(self):
        with self.lock:
            self.__cache.clear()
            self.__hits.clear()

    def keys(self):
        with self.lock:
            return self.cache.keys()

    def items(self):
        with self.lock:
            _odict = self.__cache.items()
            for key, _ in _odict:
                self.__hits[key] += 1
        return _odict

    def values(self):
        with self.lock:
            for key in self.__cache.keys():
                self.__hits[key] += 1
        return self.__cache.values()

    def hits_probs(self) -> dict:
        with self.lock:
            total = sum(self.__hits.values())
            _p = {k: v / total for k, v in self.__hits.items()}
        return dict(sorted(_p.items(), key=lambda x: x[1], reverse=False))

    def __len__(self):
        with self.lock:
            return self.cache.__len__()

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
            size += objsize.get_deep_size(self.__hits)  # instance dictionary
            for k, v in self.__hits.items():
                size += objsize.get_deep_size(k)
                size += objsize.get_deep_size(v)
        return size


