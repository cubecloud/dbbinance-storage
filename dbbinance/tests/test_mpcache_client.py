import datetime

from mlthread_tools.multiprocessing_mutex import mlp_mutex
from multiprocessing import Process, Manager, Value
import pandas as pd

from dbbinance.fetcher import MpCacheManager


def f(_obj: MpCacheManager, idnum: int):
    if 7 in _obj.cache.keys():
        with mlp_mutex:
            if isinstance(_obj.cache[7], pd.DataFrame):
                df = pd.DataFrame({'a': range(0, 9),
                                   'b': range(10, 19),
                                   'c': range(100, 109)}
                                  )
                _obj.cache[7] = df
            else:
                _obj.cache[7] = pd.DataFrame()
        print(f'id={idnum}', _obj.cache)


def a(_obj: MpCacheManager, idnum: int):
    _obj.cache.pop(0)
    print(f'id={idnum}', _obj.cache)


if __name__ == '__main__':
    cache_obj = MpCacheManager(start_host=False, host="127.0.0.1", port=5003, authkey=b"password")

    for i in range(10):
        cache_obj.update({i: i})
        cache_obj.update_cache(i ** 2, i ** 2)
    test_key = tuple((datetime.datetime.now(), datetime.datetime.now()))
    cache_obj.update_cache(test_key, 1)

    print(cache_obj.cache)

    p1 = Process(target=f, args=(cache_obj, 1))
    p2 = Process(target=f, args=(cache_obj, 2))
    p3 = Process(target=a, args=(cache_obj, 3))
    p1.start()
    p2.start()
    p3.start()
    p1.join()
    p2.join()
    p3.join()
    cache_obj.update_cache(0, 100)
    cache_obj.update({500: 500})
    print(cache_obj.items())
    # print(cache_obj.hits_probs())
    # print(cache_obj.__sizeof__())
    # print(sys.getsizeof(cache_obj.cache[7]))
    # print(objsize.get_deep_size(cache_obj))
