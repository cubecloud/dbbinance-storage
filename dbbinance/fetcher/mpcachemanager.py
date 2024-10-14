import multiprocessing

import objsize
from typing import Union
from mlthread_tools import mlp_mutex
from multiprocessing.managers import SyncManager

import logging

__version__ = 0.029

logger = logging.getLogger()


class CacheSync(SyncManager):
    pass


class MpCacheManager:
    def __init__(self,
                 max_memory_gb: Union[float, int] = 3,
                 start_host: bool = True,
                 host: str = "127.0.0.1",
                 port: int = 5003,
                 authkey: bytes = b"password",
                 mlp_rlock: Union[multiprocessing.RLock, None] = None,
                 ):
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
        if mlp_rlock is None:
            logger.warning(f"{self.__class__.__name__}: mlp_lock is None, using 'mlp_mutex'")
            self.lock = mlp_mutex
        else:
            self.lock = mlp_rlock

        self.host = host
        self.port = port
        self.authkey = authkey
        self.manager = None
        self.__cache = {}
        self.__hits = {}
        self.max_memory_bytes = int(max_memory_gb * 1024 * 1024 * 1024)  # Convert max_memory_gb to bytes
        self.host_instance = False
        if self.start_host:
            self.manager = CacheSync((self.host, self.port), authkey=self.authkey)
            self.manager.register('get_cache', callable=lambda: self.__cache)
            self.manager.register('get_hits', callable=lambda: self.__hits)
            self.manager.start()
            self.host_instance = True
        else:
            self.refresh_conn()
        self.current_memory_usage: int = 0

    def refresh_conn(self):
        self.manager = CacheSync((self.host, self.port), authkey=self.authkey)
        self.manager.register('get_cache')
        self.manager.register('get_hits')
        self.manager.connect()

    @property
    def cache(self):
        return self.manager.get_cache()

    @property
    def hits(self):
        return self.manager.get_hits()

    def update(self, key_value_dict: dict):
        self.update_cache(list(key_value_dict.keys())[0], list(key_value_dict.values())[0])

    def update_cache(self, key, value):
        """
        The cache size on host (server) must be updated by self.update_cache_size(),
        to keep cache_size in limits, cos of very slow obj.get_deep_size() recursion,
        through all cache elements to get data, if many processes trying update size simultaneously

        Args:
            key:
            value:

        Returns:
            None
        """
        with self.lock:
            value_size = objsize.get_deep_size(value) + objsize.get_deep_size(key)
            if value_size > self.max_memory_bytes:
                logger.warning(f"{self.__class__.__name__}: "
                               f"Object size is greater then {self.max_memory_bytes} increase MpCacheManager memory")
            while (self.current_memory_usage + value_size > self.max_memory_bytes) and (self.__len__() > 0):
                # Delete the oldest item to free up memory
                self.popitem(last=False)
            self.cache.update({key: value})
            self.hits.update({key: 1})
            self.current_memory_usage += (value_size + objsize.get_deep_size(key) + objsize.get_deep_size(1))

    def popitem(self, last=False):
        if last:
            idx = -1
        else:
            idx = 0
        with self.lock:
            key = self.cache.keys()[idx]
            item = self.pop(key)
        return item

    def pop(self, key):
        with self.lock:
            _ = self.hits.pop(key)
            item = self.cache.pop(key)
            self.current_memory_usage -= (
                    objsize.get_deep_size(item) + objsize.get_deep_size(key) * 2 + objsize.get_deep_size(1))
        return item

    def get(self, key, default=None):
        value = self.cache.get(key, None)
        if value is not None:
            with self.lock:
                self.hits.update({key: self.hits.get(key, 0) + 1})
        else:
            value = default
        return value

    def clear(self):
        with self.lock:
            self.cache.clear()
            self.hits.clear()

    def keys(self):
        with self.lock:
            return self.cache.keys()

    def items(self):
        with self.lock:
            _odict = self.cache.items()
            for key, _ in _odict:
                self.hits.update({key: self.hits.get(key, 0) + 1})
            return _odict

    def values(self):
        with self.lock:
            for key in self.cache.keys():
                self.hits.update({key: self.hits.get(key) + 1})
            return self.cache.values()

    def hits_probs(self) -> dict:
        with self.lock:
            total = sum(self.hits.values())
            _p = {k: v / total for k, v in self.hits.items()}
            return dict(sorted(_p.items(), key=lambda x: x[1], reverse=False))

    def __len__(self):
        """Calculating length of hits values (cos of size of list) """
        with self.lock:
            return len(self.hits.values())

    @classmethod
    def is_server_running(cls, host: str = "127.0.0.1", port: int = 5003, authkey: bytes = b"password"):
        _manager = CacheSync((host, port), authkey=authkey)
        try:
            _manager.connect()
            return True
        except ConnectionRefusedError:
            return False

    @staticmethod
    def get_cache_key(**cache_kwargs):
        return tuple(sorted(cache_kwargs.items()))

    def update_cache_size(self) -> None:
        self.current_memory_usage = self.cache_size()

    def cache_size(self) -> int:
        self.refresh_conn()
        with self.lock:
            size = objsize.get_deep_size(self.cache)  # instance dictionary
            for k, v in self.cache.items():
                size += objsize.get_deep_size(k)
                size += objsize.get_deep_size(v)
            size += objsize.get_deep_size(self.hits)  # instance dictionary
            for k, v in self.hits.items():
                size += objsize.get_deep_size(k)
                size += objsize.get_deep_size(v)
        return size

    def __del__(self):
        if self.host_instance:
            self.manager.shutdown()

    def shutdown(self):
        self.manager.shutdown()
