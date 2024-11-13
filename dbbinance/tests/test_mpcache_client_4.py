import time
from dbbinance.fetcher import MpCacheManager


if __name__ == '__main__':
    cache_obj = MpCacheManager(port=5006, start_host=False, unique_name='train')
    while True:
        data = list(cache_obj.hits_probs().items())[:10]
        hits = list(cache_obj.hits.items())[:10]
        print(f'\r{data} // {hits}', end='')
        time.sleep(5)
