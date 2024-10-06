from multiprocessing.managers import SyncManager
from dbbinance.fetcher.mpcachemanager import MpCacheManager
import time

if __name__ == "__main__":
    if not MpCacheManager.is_server_running(port=5500):
        man_obj = MpCacheManager(port=5500)
        print("Shared dictionary created:", man_obj.items())
        man_obj.update({800: 800})
        cache = list(man_obj.items())
        # print(type(man_obj.keys()))
        # print(type(man_obj.keys()[0]))

        # while cache == man_obj.items():
        #     print(f'\r{man_obj.items()}', end='')
        #     time.sleep(5)
        while True:
            print(f'\r{man_obj.items()} -> length = {len(man_obj)}', end='')
            time.sleep(5)
        # print(f'\n{man_obj.items()}')
        # man_obj.shutdown()
    else:
        print('Server is running')


