from threading import RLock

import objsize
from dbbinance.fetcher.slocks import SThLock, SMpLock
from dbbinance.fetcher.singleton import Singleton
from typing import Union, Any, Dict
import numpy as np
import multiprocessing as mp
from multiprocessing.managers import SyncManager

__version__ = 0.042

logger = mp.get_logger()


class CacheSync(SyncManager):
    pass


class PERCacheManager(metaclass=Singleton):
    def __init__(self,
                 max_memory_gb: Union[float, int] = 3,
                 start_host: bool = True,
                 host: str = "127.0.0.1",
                 port: int = 5003,
                 authkey: bytes = b"password",
                 th_rlock=None,
                 unique_name='train'
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
        self.host = host
        self.port = port
        self.unique_name = f'{unique_name}'

        self.manager = None
        self.__cache = {}
        self.__hits = {}
        self.__score = {}

        self._keys = []
        self.total_priority = 0.0  # Total sum of priorities
        self.alpha = 0.9  # weight of priorities
        self.beta = 0.7  # weight for hits
        self.probabilities = np.empty((0,))

        # self.max_memory_bytes = int(max_memory_gb * 1024 * 1024 * 1024)  # Convert max_memory_gb to bytes
        self.host_instance = False
        self.manager = CacheSync((self.host, self.port), authkey=authkey)
        self.thrlock_obj = SThLock(th_rlock if th_rlock is not None else RLock(),
                                   unique_name=f'{self.unique_name}_rlock')
        self.lock = self.thrlock_obj.lock
        if self.start_host:
            """
            Start host instance 
            using Threading lock (SThLock) to avoid race conditions with multiple clients 
            in one process
            SyncManager is not thread-safe, but for multiprocessing it is  
            """
            self.manager.register('get_cache', callable=lambda: self.__cache)
            self.manager.register('get_hits', callable=lambda: self.__hits)
            self.manager.register('get_score', callable=lambda: self.__score)
            self.manager.start()
            self.host_instance = True
        else:
            """
            Connect to host instance
            using Threading lock (SThLock) to avoid race conditions with multiple clients 
            in one process
            SyncManager is not thread-safe, but for multiprocessing it is
            """
            self.manager.register('get_cache')
            self.manager.register('get_hits')
            self.manager.register('get_score')
            self.manager.connect()
            self.host_instance = False
        self.current_memory_usage: int = 0

    def set_thrlock(self, rlock_obj):
        self.thrlock_obj = rlock_obj
        self.lock = self.thrlock_obj.lock

    @property
    def cache(self):
        return self.manager.get_cache()

    @property
    def hits(self):
        return self.manager.get_hits()

    @property
    def score(self):
        return self.manager.get_score()

    def update(self, key_value_dict: dict):
        for key, value in key_value_dict.items():
            self.update_cache(key, value)

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
            self.cache.update({key: value})
            self.hits.update({key: 1})
            self.score.update({key: 0.0})
            self._update_total_priority(1.0)

    def update_score(self, key, new_score) -> None:
        """ Update score for key """
        with self.lock:
            if key not in self.score.keys():
                raise KeyError(f"Key {key} not present in cache")
            delta_score = new_score - self.score.get(key)
            self.score.update({key: new_score})
            self._update_total_priority(delta_score)

    def _update_total_priority(self, score) -> None:
        """ Update total priority """
        with self.lock:
            self.total_priority += pow(score, self.alpha)

    def popitem(self, last=False) -> Any:
        if last:
            idx = -1
        else:
            idx = 0
        with self.lock:
            key = self.cache.keys()[idx]
            item = self.pop(key)
        return item

    def pop(self, key) -> Any:
        with self.lock:
            _ = self.hits.pop(key)
            _ = self.score.pop(key)
            item = self.cache.pop(key)
        return item

    def get(self, key, default=None) -> Any:
        with self.lock:
            value = self.cache.get(key, None)
            if value is not None:
                self.hits.update({key: self.hits.get(key, 0) + 1})
            else:
                value = default
        return value

    def clear(self) -> None:
        with self.lock:
            self.cache.clear()
            self.hits.clear()
            self.score.clear()

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

    def hits_probs(self) -> Dict:
        with self.lock:
            total = sum(self.hits.values())
            _p = {k: v / total for k, v in self.hits.items()}
            return dict(sorted(_p.items(), key=lambda x: x[1], reverse=False))

    def score_probs(self) -> Dict:
        """
        Returns a Dict of keys and probabilities sorted
        from higher (less scored) to lower (high scored)
        probabilities to learn with PER principles
        """
        with self.lock:
            scores = []
            hits = []
            _keys = []
            hits_copy = {k: v for k, v in self.hits.items()}

            # synchronize score and hits by keys
            for key, score in self.score.items():
                _keys.append(key)
                scores.append(score)
                hits.append(hits_copy[key])

            scores = np.asarray(scores, dtype=float)
            hits = np.asarray(hits, dtype=float)

            scores = scores / np.mean(scores)
            hits = hits / np.mean(hits)

            # calculate priorities
            priorities = np.power(scores, self.alpha) * np.power(hits, self.beta)

            # Normalize priorities and probabilities
            total_priority = np.sum(priorities)
            probabilities = priorities / total_priority

            sampled_indices = np.argsort(probabilities)

            sorted_data = {_keys[idx]: probabilities[idx] for idx in sampled_indices}

        return sorted_data

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
        with self.lock:
            size = objsize.get_deep_size(self.cache)  # instance dictionary
            for k, v in self.cache.items():
                size += objsize.get_deep_size(k)
                size += objsize.get_deep_size(v)
            size += objsize.get_deep_size(self.hits)  # instance dictionary
            for k, v in self.hits.items():
                size += objsize.get_deep_size(k)
                size += objsize.get_deep_size(v)
            size += objsize.get_deep_size(self.hits)  # instance dictionary
            for k, v in self.score.items():
                size += objsize.get_deep_size(k)
                size += objsize.get_deep_size(v)
        return size

    def __del__(self):
        if self.host_instance:
            self.manager.shutdown()

    def shutdown(self):
        self.manager.shutdown()
