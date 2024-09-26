from multiprocessing.managers import SyncManager
import multiprocessing
import logging
import time

logger = multiprocessing.log_to_stderr()

logger.setLevel(logging.INFO)


def create_shared_dict():
    manager = SyncManager(address=('127.0.0.1', 5007), authkey=b'secret')
    shared_dict = dict({'key1': 'value1', 'key2': 'value2'})

    manager.register('shared_dict', callable=lambda: shared_dict)
    manager.start()
    print("Shared dictionary created:", shared_dict)
    b = 0
    while list(shared_dict.items()) == manager.shared_dict().items():
        print(f'\r{manager.shared_dict().items()}', end='')
        time.sleep(5)
    print(manager.shared_dict().items())
    manager.shutdown()


if __name__ == '__main__':
    create_shared_dict()
