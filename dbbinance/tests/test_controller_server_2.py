from dbbinance.fetcher.rlcontroller import Controller
from threading import RLock
import time

if __name__ == "__main__":
    lock = RLock()
    if not Controller.is_server_running(port=5500):
        man_obj = Controller(port=5500, unique_name='train', th_rlock=lock)
        print("Shared dictionary created:", man_obj.items())
        man_obj.update({800: 800})
        print("id_counter:",  man_obj.id_counter)
        man_obj.next_id_counter()
        # print("id_counter:",  man_obj.id_counter)
        # print("Value:",  man_obj.Value.value)
        # cache = list(man_obj.items())
        # print(type(man_obj.keys()))
        # print(type(man_obj.keys()[0]))

        # while cache == man_obj.items():
        #     print(f'\r{man_obj.items()}', end='')
        #     time.sleep(5)
        while True:
            print(f'\r{man_obj.hits_probs()} {man_obj.items()} -> length = {len(man_obj)}', end='')
            time.sleep(20)
        # print(f'\n{man_obj.items()}')
        # man_obj.shutdown()
    else:
        print('Server is running')


