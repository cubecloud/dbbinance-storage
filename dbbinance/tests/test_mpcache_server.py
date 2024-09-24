from multiprocessing.managers import SyncManager
from dbbinance.fetcher.mpcachemanager import MpCacheManager


if __name__ == "__main__":
    man_obj = MpCacheManager()
    input('Waiting for any key, to server "shutdown"')
    man_obj.shutdown()

