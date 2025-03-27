from dbbinance.fetcher import MpCacheManager
import pandas as pd

if __name__ == '__main__':
    def count_timeframes(start, end, freq):
        index = pd.date_range(start=start, end=end, freq=freq)
        return len(index)


    cache_obj = MpCacheManager(port=5005, start_host=False, unique_name='train')
    results = []
    data = list(cache_obj.keys())
    print(len(data))
    data = sorted(data, key=lambda x: x[0])
    # print(f'{sorted(data, key= lambda x: x[0])}')
    for start, end in data:
        results.append((start, end, (count_timeframes(start, end, freq=start.freqstr))))
    print(results)

    min_start = min(data, key=lambda x: x[0])[0]
    max_end = max(data, key=lambda x: x[1])[1]

    print("Min start:", min_start)
    print("Max end:", max_end)

    cache_obj = MpCacheManager(port=5006, start_host=False, unique_name='test')
    data = list(cache_obj.keys())
    print(len(data))
    print(f'{data}')
    min_start = min(data, key=lambda x: x[0])[0]
    max_end = max(data, key=lambda x: x[1])[1]

    print("Min start:", min_start)
    print("Max end:", max_end)

