import multiprocessing as mp
from multiprocessing.managers import BaseManager, SyncManager, BaseProxy, Namespace, ValueProxy
import threading

from dbbinance.fetcher.singleton import Singleton


class CacheSync(SyncManager):
    pass


# class NamespaceProxy(BaseProxy):
#     _exposed_ = ('__getattribute__', '__setattr__', '__delattr__')
#
#     def __getattr__(self, key):
#         if key[0] == '_':
#             return object.__getattribute__(self, key)
#         callmethod = object.__getattribute__(self, '_callmethod')
#         return callmethod('__getattribute__', (key,))
#
#     def __setattr__(self, key, value):
#         if key[0] == '_':
#             return object.__setattr__(self, key, value)
#         callmethod = object.__getattribute__(self, '_callmethod')
#         return callmethod('__setattr__', (key, value))
#
#     def __delattr__(self, key):
#         if key[0] == '_':
#             return object.__delattr__(self, key)
#         callmethod = object.__getattribute__(self, '_callmethod')
#         return callmethod('__delattr__', (key,))


mp_count = mp.Value('i', 0)


# class Control:
#     count = None
#
#     @staticmethod
#     def check_globals():
#         Control.count = globals().get('mp_count', None)
#
#     @staticmethod
#     def set_count(ref):
#         Control.count = ref

class MainControls:
    counter = 0

    def __init__(self):
        self.lock = threading.RLock()
        self._id_counter = MainControls.counter

    def get_id_counter(self) -> int:
        with self.lock:
            return self._id_counter

    def set_id_counter(self, new_value):
        with self.lock:
            self._id_counter = new_value

    def increment_id_counter(self):
        with self.lock:
            self._id_counter += 1
            return self._id_counter


class Controller(MainControls, metaclass=Singleton):
    def __init__(self,
                 start_host: bool = True,
                 host: str = "127.0.0.1",
                 port: int = 5003,
                 authkey: bytes = b"password",
                 unique_name='train'):
        MainControls.__init__(self)
        self.manager = CacheSync(address=(host, port), authkey=authkey)
        self.start()
        self._id_counter = self.manager.increment_id_counter()

    #     self.lock = threading.RLock()
    #
    # def get_id_counter(self) -> int:
    #     with self.lock:
    #         return self._id_counter
    #
    # def set_id_counter(self, new_value):
    #     with self.lock:
    #         self._id_counter = new_value
    #
    # def increment_id_counter(self):
    #     with self.lock:
    #         self._id_counter += 1
    #         return self._id_counter

    def start(self, **kwargs):
        self.manager.register('get_id_counter', self.get_id_counter)
        self.manager.register('set_id_counter', callable=self.set_id_counter)
        self.manager.register('increment_id_counter', callable=self.increment_id_counter)
        self.manager.start()

    def shutdown(self):
        self.manager.shutdown()


if __name__ == '__main__':
    while True:
        controller = Controller(address=("127.0.0.1", 5990), authkey=b'password', unique_name='test')
        controller.start()
        pressed = input("Press Q to stop...")
        if pressed == 'Q':
            controller.shutdown()
            print('Shutdown controller...')
            break
        else:
            print('Restarting controller...')
