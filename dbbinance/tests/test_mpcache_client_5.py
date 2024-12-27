from dbbinance.fetcher import MpCacheManager


if __name__ == '__main__':
    cache_obj = MpCacheManager(port=5005, start_host=False, unique_name='train')
    data = list(cache_obj.keys())
    print(len(data))
    # print(f'{data}')
    min_start = min(data, key=lambda x: x[0])[0]
    max_end = max(data, key=lambda x: x[1])[1]

    print("Min start:", min_start)
    print("Max end:", max_end)

    cache_obj = MpCacheManager(port=5006, start_host=False, unique_name='test')
    data = list(cache_obj.keys())
    print(len(data))
    # print(f'{data}')
    min_start = min(data, key=lambda x: x[0])[0]
    max_end = max(data, key=lambda x: x[1])[1]

    print("Min start:", min_start)
    print("Max end:", max_end)

