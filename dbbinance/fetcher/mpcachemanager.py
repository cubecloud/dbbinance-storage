import sys
import objsize
from typing import Union
from collections import OrderedDict
from mlthread_tools import mlp_mutex
from multiprocessing.managers import SyncManager
import logging

__version__ = 0.025

logger = logging.getLogger()


class MpCacheManager:
    def __init__(self,
                 max_memory_gb: Union[float, int] = 3,
                 start_host: bool = True,
                 host: str = "127.0.0.1",
                 port: int = 5003,
                 authkey: bytes = b"password"):
        """
        Initialize the Cache class with an optional maximum memory limit in gigabytes.

        Args:
            max_memory_gb (float or int):   The maximum memory limit in gigabytes.
            start_host (bool):              Start host or just connect to it
            host (str):                     Host IP
            port (int):                     Host port
            authkey (bytes):                Authorization password (bytes)
        """
        self.start_host = start_host
        self.lock = mlp_mutex
        self.host = host
        self.port = port
        self.authkey = authkey
        self.manager = None
        self.__cache = None
        self.__hits = None
        self.max_memory_bytes = int(max_memory_gb * 1024 * 1024 * 1024)  # Convert max_memory_gb to bytes
        self.host_instance = False
        if self.start_host:
            self.manager = SyncManager((self.host, self.port), authkey=self.authkey)
            self.manager.start()
            self.host_instance = True
        else:
            self.manager = SyncManager((self.host, self.port), authkey=authkey)
            self.manager.connect()

        self.__cache = self.manager.dict()
        self.__hits = self.manager.dict()
        self.current_memory_usage = 0

    @property
    def cache(self):
        return self.__cache

    def update(self, key_value_dict: dict):
        self.update_cache(list(key_value_dict.keys())[0], list(key_value_dict.values())[0])

    def update_cache(self, key, value):
        with self.lock:
            value_size = objsize.get_deep_size(value) + objsize.get_deep_size(key)
            if value_size > self.max_memory_bytes:
                logger.warning(f"{self.__class__.__name__}: "
                               f"Object size is greater then {self.max_memory_bytes} increase CacheManager memory")
            while (self.current_memory_usage + value_size > self.max_memory_bytes) and len(self.__cache) > 0:
                # Delete the oldest item to free up memory
                self.popitem(last=False)
            self.__cache.update({key: value})
            self.__hits.update({key: 0})
            self.current_memory_usage = self.cache_size()

    def popitem(self, last=False):
        with self.lock:
            if last:
                idx = -1
            else:
                idx = 0
            key = self.__cache.keys()[idx]
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
            if key not in self.__hits:
                with self.lock:
                    self.__hits[key] = 0
            with self.lock:
                self.__hits[key] += 1
        else:
            value = default
        return value

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

    @staticmethod
    def get_cache_key(**cache_kwargs):
        return tuple(sorted(cache_kwargs.items()))

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

    def __del__(self):
        if self.host_instance:
            self.manager.shutdown()

    def shutdown(self):
        self.manager.shutdown()
