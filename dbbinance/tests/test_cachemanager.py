from collections import OrderedDict
from dbbinance.fetcher.cachemanager import CacheManager, CacheDict


if __name__ == "__main__":
    cd_test = CacheDict()
    od_test = OrderedDict()
    for i in range(10):
        cd_test.update({i: i})
        od_test.update({i: i})
    cd_test.pop(3)
    b = cd_test.get(5, None)
    print(b)
    for item in cd_test.items():
        print(item)
    print(cd_test.hits)
    print(cd_test.items())
    print(cd_test.hits)
    print(cd_test.values())
    print(cd_test.hits)
    print(cd_test[9])
    print(cd_test.hits)
    print(cd_test.__sizeof__())
    cd_test.update({12: 12})
    print(cd_test.hits)
    print(cd_test.get(12))
    print(cd_test.hits)
    print(cd_test.hits_probs())
    cm = CacheManager()
    for i in range(10):
        cm.update({i: i})
        cm.update_cache(i*2, i*2)
    print(len(cm))
    print(cm.items())

