from multiprocessing.managers import SyncManager
from dbbinance.fetcher.mpcachemanager import MpCacheManager
import time

if __name__ == "__main__":
    man_obj = MpCacheManager()
    print("Shared dictionary created:", man_obj.items())
    man_obj.update({800: 800})
    cache = list(man_obj.items())
    # while cache == man_obj.items():
    #     print(f'\r{man_obj.items()}', end='')
    #     time.sleep(5)
    while True:
        print(f'\r{man_obj.items()}', end='')
        time.sleep(5)
    # print(f'\n{man_obj.items()}')
    # man_obj.shutdown()



