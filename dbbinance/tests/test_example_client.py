from multiprocessing.managers import SyncManager
import multiprocessing
import logging

if __name__ == '__main__':
    logger = multiprocessing.log_to_stderr()

    logger.setLevel(logging.DEBUG)

    host = '127.0.0.1'  # IP address of the server
    port = 5007  # Port number of the server
    manager = SyncManager(address=(host, port), authkey=b'secret')
    manager.register('shared_dict')
    manager.connect()
    shared_dict = manager.shared_dict()
    print(type(shared_dict))
    print("Shared dictionary accessed:", shared_dict)
    manager.shared_dict().update({'key3': 'value3'})
    print("Updated shared dictionary:", manager.shared_dict())
